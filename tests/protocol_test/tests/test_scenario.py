"""Scenario-driven tests — add new test cases by writing YAML, not Python.

Each test loads a YAML scenario file and feeds it to the ScenarioRunner.
To add a new scenario: create a YAML in scenarios/full/ and add a one-liner here.
"""

from pathlib import Path

import pytest

from framework.scenario_runner import ScenarioRunner

SCENARIO_DIR = Path(__file__).parents[1] / "scenarios"


@pytest.fixture
def runner(binary_dir, tmp_path):
    return ScenarioRunner(
        binary_dir=str(binary_dir) if binary_dir is not None else None,
        log_dir=str(tmp_path / "logs"),
    )


class TestScenario:
    """YAML-driven integration test scenarios."""

    def test_kill_ds_and_verify_rebalance(self, runner):
        """Kill one DS → DEAD → shard rebalance → no orphaned shards."""
        runner.run_scenario(
            SCENARIO_DIR / "full" / "kill_ds_and_verify_rebalance.yaml"
        )

    def test_cm_restart_ds_rejoin(self, runner):
        """Kill CM → restart → all DS re-register → shard table rebuilt."""
        runner.run_scenario(
            SCENARIO_DIR / "full" / "cm_restart_ds_rejoin.yaml"
        )

    def test_composable_scenario(self, runner):
        """Demonstrates YAML composition: small cluster + fast heartbeat + kill fault."""
        runner.run_scenario(
            SCENARIO_DIR / "clusters" / "small.yaml",
            SCENARIO_DIR / "overrides" / "fast_heartbeat.yaml",
            SCENARIO_DIR / "faults" / "kill_one_ds.yaml",
        )
        # Note: this scenario has faults but no validations section,
        # so it only verifies that the cluster survives the fault without crashing.
