"""YAML configuration loading and merging for test scenarios.

Field names in ClusterConfig correspond to SiMM gflag names:
  CM flags (cm_flags.cc):
    cm_cluster_init_grace_period_inSecs  (default 60)
    cm_heartbeat_timeout_inSecs          (default 30)
    cm_heartbeat_bg_scan_interval_inSecs (default 5)
  DS flags (ds_flags.cc):
    heartbeat_cooldown_sec               (default 5)
    register_cooldown_sec                (default 10)
    cm_hb_tolerance_count                (default 5)
    cm_connect_retry_interval_sec        (default 1)
    memory_limit_bytes                   (default 0 = auto)
  Common flags:
    shard_total_num
"""

import copy
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class HostConfig:
    """Configuration for a single host (machine) in the cluster."""
    ip: str
    binary_dir: str = ""
    log_dir: str = "/tmp/simm_test_logs"


@dataclass
class ClusterConfig:
    """Configuration for a test cluster.

    Field names match SiMM gflag names where applicable.
    """
    num_data_servers: int = 3
    shard_total_num: int = 64

    # CM flags — names match cm_flags.cc exactly
    cm_cluster_init_grace_period_inSecs: int = 5
    cm_heartbeat_timeout_inSecs: int = 10
    cm_heartbeat_bg_scan_interval_inSecs: int = 2

    # DS flags — names match ds_flags.cc exactly
    heartbeat_cooldown_sec: int = 2
    register_cooldown_sec: int = 2
    cm_hb_tolerance_count: int = 5
    cm_connect_retry_interval_sec: int = 1
    memory_limit_bytes: int = 1 << 30

    # Deferred Reshard flags — names match cm_flags.cc / ds_flags.cc
    cm_deferred_reshard_enabled: bool = True
    cm_deferred_reshard_window_inSecs: int = 120
    ds_logical_node_id_prefix: str = "simm-ds"  # test helper: DS-{i} gets "{prefix}-{i}"

    # Build
    binary_dir: str = "/opt/simm/build/release/bin"
    build_mode: str = "release"

    # Host topology — None means single-machine mode
    cm_host: HostConfig | None = None
    ds_hosts: list[HostConfig] = field(default_factory=list)

    # SSH settings
    ssh_user: str = ""
    ssh_port: int = 22

    # --- Derived timeout helpers ---

    @property
    def cm_failure_detection_max_sec(self) -> float:
        """Max time for CM to detect a dead DS:
        cm_heartbeat_timeout_inSecs + 2 * cm_heartbeat_bg_scan_interval_inSecs"""
        return (self.cm_heartbeat_timeout_inSecs
                + 2 * self.cm_heartbeat_bg_scan_interval_inSecs)

    @property
    def ds_cm_failure_detection_sec(self) -> float:
        """Time for DS to detect CM is down:
        cm_hb_tolerance_count * heartbeat_cooldown_sec"""
        return self.cm_hb_tolerance_count * self.heartbeat_cooldown_sec

    @property
    def ds_rejoin_max_sec(self) -> float:
        """Max time for a DS to re-register after detecting CM failure:
        ds_cm_failure_detection_sec + register_cooldown_sec + cm_connect_retry_interval_sec * retries"""
        return (self.ds_cm_failure_detection_sec
                + self.register_cooldown_sec
                + 15)  # buffer for retries


@dataclass
class FaultConfig:
    """Configuration for a fault injection step."""
    fault_type: str
    targets: list[str] = field(default_factory=list)
    delay_after_sec: float = 0
    duration_sec: float | None = None


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge two dicts. Override values take precedence."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def merge_yaml_files(*paths: Path) -> dict:
    result: dict = {}
    for p in paths:
        data = load_yaml(p)
        result = deep_merge(result, data)
    return result


def _parse_host_config(data: dict) -> HostConfig:
    return HostConfig(
        ip=data["ip"],
        binary_dir=data.get("binary_dir", ""),
        log_dir=data.get("log_dir", "/tmp/simm_test_logs"),
    )


def dict_to_cluster_config(data: dict) -> ClusterConfig:
    """Convert a YAML dict to ClusterConfig."""
    cluster = data.get("cluster", {})
    cm = cluster.get("cluster_manager", {})
    ds = cluster.get("data_servers", {})
    hosts = cluster.get("hosts", {})
    ssh = cluster.get("ssh", {})

    cm_host = None
    ds_hosts = []
    if "cm" in hosts:
        cm_host = _parse_host_config(hosts["cm"])
    if "ds" in hosts:
        ds_hosts = [_parse_host_config(h) for h in hosts["ds"]]

    return ClusterConfig(
        num_data_servers=ds.get("count", 3),
        shard_total_num=cluster.get("shard_total_num", 64),
        cm_cluster_init_grace_period_inSecs=cm.get("cm_cluster_init_grace_period_inSecs", 5),
        cm_heartbeat_timeout_inSecs=cm.get("cm_heartbeat_timeout_inSecs", 10),
        cm_heartbeat_bg_scan_interval_inSecs=cm.get("cm_heartbeat_bg_scan_interval_inSecs", 2),
        heartbeat_cooldown_sec=ds.get("heartbeat_cooldown_sec", 2),
        register_cooldown_sec=ds.get("register_cooldown_sec", 2),
        cm_hb_tolerance_count=ds.get("cm_hb_tolerance_count", 5),
        cm_connect_retry_interval_sec=ds.get("cm_connect_retry_interval_sec", 1),
        memory_limit_bytes=ds.get("memory_limit_bytes", 1 << 30),
        cm_deferred_reshard_enabled=cm.get("cm_deferred_reshard_enabled", True),
        cm_deferred_reshard_window_inSecs=cm.get("cm_deferred_reshard_window_inSecs", 120),
        ds_logical_node_id_prefix=ds.get("ds_logical_node_id_prefix", "simm-ds"),
        binary_dir=cluster.get("binary_dir", "/opt/simm/build/release/bin"),
        build_mode=cluster.get("build_mode", "release"),
        cm_host=cm_host,
        ds_hosts=ds_hosts,
        ssh_user=ssh.get("user", ""),
        ssh_port=ssh.get("port", 22),
    )


def dict_to_fault_configs(data: dict) -> list[FaultConfig]:
    faults = data.get("faults", [])
    result = []
    for f in faults:
        targets = f.get("targets", [])
        if isinstance(f.get("target"), str):
            targets = [f["target"]]
        result.append(FaultConfig(
            fault_type=f["type"],
            targets=targets,
            delay_after_sec=f.get("delay_after_ready_sec", 0),
            duration_sec=f.get("duration_sec"),
        ))
    return result


def load_scenario(scenario_dir: Path, *yaml_files: str) -> tuple[ClusterConfig, list[FaultConfig]]:
    paths = [scenario_dir / f for f in yaml_files]
    merged = merge_yaml_files(*paths)
    return dict_to_cluster_config(merged), dict_to_fault_configs(merged)
