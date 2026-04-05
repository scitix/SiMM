"""Scenario-driven test orchestration engine.

The ScenarioRunner reads YAML scenario definitions and executes them:
  1. Start cluster (from cluster config)
  2. Wait for cluster ready
  3. For each fault step:
     a. Inject fault (with optional delay)
     b. Wait for expected state transitions
  4. Run validation checks
  5. Teardown

This makes adding new test scenarios declarative — write YAML, not Python.

YAML scenario format:
  cluster:
    # ... cluster config (same as before)
  faults:
    - type: sigkill | sigterm | sigstop | sigcont
      target: "ds:0" | "cm"
      delay_after_ready_sec: 5
      duration_sec: 15           # for sigstop only
  validations:
    - type: node_status
      target: "ds:0"
      expected: DEAD
      within_sec: 20             # max wait time
    - type: shard_total
      expected: 64
    - type: shard_balance
      max_imbalance_ratio: 0.3
    - type: no_orphaned_shards
    - type: node_has_no_shards
      target: "ds:0"
    - type: alive_node_count
      expected: 2
    - type: ds_status               # DS admin RPC check
      target: "ds:0"
      field: cm_ready
      expected: "false"
    - type: all_nodes_running
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from .cluster import SimmCluster
from .config import (
    ClusterConfig, FaultConfig,
    load_yaml, deep_merge, dict_to_cluster_config, dict_to_fault_configs,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation step definitions
# ---------------------------------------------------------------------------

@dataclass
class ValidationStep:
    """A single validation check to run after faults are injected."""
    type: str                         # validation type name
    target: str = ""                  # e.g. "ds:0", "cm"
    expected: str = ""                # expected value (type-dependent)
    field: str = ""                   # for ds_status: which field to check
    within_sec: float = 60            # max wait time for condition-based checks
    max_imbalance_ratio: float = 0.3  # for shard_balance


def _parse_validation_steps(data: dict) -> list[ValidationStep]:
    """Parse validation steps from YAML dict."""
    steps = []
    for v in data.get("validations", []):
        steps.append(ValidationStep(
            type=v["type"],
            target=v.get("target", ""),
            expected=str(v.get("expected", "")),
            field=v.get("field", ""),
            within_sec=v.get("within_sec", 60),
            max_imbalance_ratio=v.get("max_imbalance_ratio", 0.3),
        ))
    return steps


# ---------------------------------------------------------------------------
# Fault executor — maps YAML fault type to FaultInjector calls
# ---------------------------------------------------------------------------

_FAULT_REGISTRY: dict[str, callable] = {}


def register_fault(name: str):
    """Decorator to register a fault executor function."""
    def decorator(fn):
        _FAULT_REGISTRY[name] = fn
        return fn
    return decorator


@register_fault("sigkill")
def _exec_sigkill(cluster: SimmCluster, fault: FaultConfig):
    for target_str in fault.targets:
        handle = _resolve_target(cluster, target_str)
        cluster.fault_injector.kill_process(handle)


@register_fault("sigterm")
def _exec_sigterm(cluster: SimmCluster, fault: FaultConfig):
    for target_str in fault.targets:
        handle = _resolve_target(cluster, target_str)
        cluster.fault_injector.graceful_stop(handle)


@register_fault("sigstop")
def _exec_sigstop(cluster: SimmCluster, fault: FaultConfig):
    for target_str in fault.targets:
        handle = _resolve_target(cluster, target_str)
        cluster.fault_injector.freeze_process(handle)


@register_fault("sigcont")
def _exec_sigcont(cluster: SimmCluster, fault: FaultConfig):
    for target_str in fault.targets:
        handle = _resolve_target(cluster, target_str)
        cluster.fault_injector.unfreeze_process(handle)


@register_fault("kill_multiple")
def _exec_kill_multiple(cluster: SimmCluster, fault: FaultConfig):
    handles = [_resolve_target(cluster, t) for t in fault.targets]
    cluster.fault_injector.kill_multiple(handles, simultaneous=True)


@register_fault("restart_cm")
def _exec_restart_cm(cluster: SimmCluster, fault: FaultConfig):
    cluster.fault_injector.kill_process(cluster.cm)
    time.sleep(fault.duration_sec or 3)
    cluster.restart_cm()


def _resolve_target(cluster: SimmCluster, target_str: str):
    """Resolve 'ds:0', 'ds:1', 'cm' to a ProcessHandle."""
    if target_str == "cm":
        return cluster.cm
    if target_str.startswith("ds:"):
        idx = int(target_str.split(":")[1])
        return cluster.get_ds_handle(idx)
    raise ValueError(f"Unknown target: {target_str}")


# ---------------------------------------------------------------------------
# Validation executor — maps YAML validation type to observer assertions
# ---------------------------------------------------------------------------

_VALIDATION_REGISTRY: dict[str, callable] = {}


def register_validation(name: str):
    """Decorator to register a validation executor function."""
    def decorator(fn):
        _VALIDATION_REGISTRY[name] = fn
        return fn
    return decorator


@register_validation("node_status")
def _validate_node_status(cluster: SimmCluster, step: ValidationStep):
    handle = _resolve_target(cluster, step.target)
    assert cluster.observer.wait_for_node_status(
        handle.addr_str, step.expected, timeout=step.within_sec
    ), (f"Node {handle.addr_str} did not reach status {step.expected} "
        f"within {step.within_sec}s")


@register_validation("alive_node_count")
def _validate_alive_count(cluster: SimmCluster, step: ValidationStep):
    expected = int(step.expected)
    assert cluster.observer.wait_for_node_count(
        expected, timeout=step.within_sec
    ), f"Alive node count did not reach {expected} within {step.within_sec}s"


@register_validation("all_nodes_running")
def _validate_all_running(cluster: SimmCluster, step: ValidationStep):
    cluster.observer.assert_all_nodes_running()


@register_validation("shard_total")
def _validate_shard_total(cluster: SimmCluster, step: ValidationStep):
    expected = int(step.expected)
    cluster.observer.assert_total_shard_count(expected)


@register_validation("shard_balance")
def _validate_shard_balance(cluster: SimmCluster, step: ValidationStep):
    cluster.observer.assert_shard_balance(
        max_imbalance_ratio=step.max_imbalance_ratio
    )


@register_validation("no_orphaned_shards")
def _validate_no_orphaned(cluster: SimmCluster, step: ValidationStep):
    cluster.observer.wait_for_rebalance_complete(timeout=step.within_sec)
    cluster.observer.assert_no_orphaned_shards()


@register_validation("node_has_no_shards")
def _validate_node_no_shards(cluster: SimmCluster, step: ValidationStep):
    handle = _resolve_target(cluster, step.target)
    cluster.observer.assert_node_has_no_shards(handle.addr_str)


@register_validation("ds_status")
def _validate_ds_status(cluster: SimmCluster, step: ValidationStep):
    """Validate a DS internal status field via admin RPC.
    target: 'ds:0', field: 'cm_ready'/'is_registered'/'heartbeat_failure_count',
    expected: the expected value as string.
    """
    handle = _resolve_target(cluster, step.target)
    status = cluster.observer.get_ds_status(handle.ip, handle.ports["admin"])
    actual = status.get(step.field, "")
    assert actual == step.expected, (
        f"DS {handle.addr_str} {step.field}={actual}, expected {step.expected}"
    )


# ---------------------------------------------------------------------------
# ScenarioRunner
# ---------------------------------------------------------------------------

class ScenarioRunner:
    """
    Orchestration engine that executes a test scenario from YAML definition.

    Usage:
        runner = ScenarioRunner()
        runner.run_scenario("scenarios/clusters/small.yaml",
                           "scenarios/faults/kill_one_ds.yaml",
                           "scenarios/validations/check_rebalance.yaml")

    Or from a single combined YAML:
        runner.run_scenario("scenarios/full/kill_ds_and_verify.yaml")

    Or programmatically:
        runner.run(cluster_config, fault_configs, validation_steps)
    """

    def __init__(self, build_dir: str | None = None, log_dir: str | None = None):
        self._build_dir = build_dir
        self._log_dir = log_dir

    def run_scenario(self, *yaml_paths: str | Path) -> None:
        """Load and execute a scenario from one or more YAML files (merged in order)."""
        merged: dict = {}
        for p in yaml_paths:
            data = load_yaml(Path(p))
            merged = deep_merge(merged, data)

        cluster_config = dict_to_cluster_config(merged)
        fault_configs = dict_to_fault_configs(merged)
        validation_steps = _parse_validation_steps(merged)

        self.run(cluster_config, fault_configs, validation_steps)

    def run(self, cluster_config: ClusterConfig,
            fault_configs: list[FaultConfig],
            validation_steps: list[ValidationStep]) -> None:
        """Execute a scenario programmatically."""
        cluster = SimmCluster(
            cluster_config,
            log_dir=self._log_dir,
            build_dir=self._build_dir,
        )

        try:
            # Phase 1: Start cluster
            logger.info("=== Phase 1: Starting cluster ===")
            cluster.start()
            cluster.wait_ready()

            # Phase 2: Inject faults
            logger.info("=== Phase 2: Injecting %d fault(s) ===", len(fault_configs))
            for i, fault in enumerate(fault_configs):
                if fault.delay_after_sec > 0:
                    logger.info("Waiting %.1fs before fault #%d", fault.delay_after_sec, i)
                    time.sleep(fault.delay_after_sec)

                executor = _FAULT_REGISTRY.get(fault.fault_type)
                if executor is None:
                    raise ValueError(
                        f"Unknown fault type: {fault.fault_type}. "
                        f"Registered: {list(_FAULT_REGISTRY.keys())}"
                    )

                logger.info("Injecting fault #%d: %s → %s",
                            i, fault.fault_type, fault.targets)
                executor(cluster, fault)

                # For sigstop with duration: wait, then sigcont
                if fault.fault_type == "sigstop" and fault.duration_sec:
                    logger.info("Waiting %.1fs then unfreezing", fault.duration_sec)
                    time.sleep(fault.duration_sec)
                    for target_str in fault.targets:
                        handle = _resolve_target(cluster, target_str)
                        cluster.fault_injector.unfreeze_process(handle)

            # Phase 3: Run validations
            logger.info("=== Phase 3: Running %d validation(s) ===", len(validation_steps))
            for i, step in enumerate(validation_steps):
                validator = _VALIDATION_REGISTRY.get(step.type)
                if validator is None:
                    raise ValueError(
                        f"Unknown validation type: {step.type}. "
                        f"Registered: {list(_VALIDATION_REGISTRY.keys())}"
                    )

                logger.info("Validation #%d: %s (target=%s, expected=%s)",
                            i, step.type, step.target, step.expected)
                validator(cluster, step)
                logger.info("Validation #%d: PASSED", i)

            logger.info("=== Scenario PASSED ===")

        finally:
            cluster.teardown()

    @staticmethod
    def list_fault_types() -> list[str]:
        """List all registered fault types."""
        return list(_FAULT_REGISTRY.keys())

    @staticmethod
    def list_validation_types() -> list[str]:
        """List all registered validation types."""
        return list(_VALIDATION_REGISTRY.keys())
