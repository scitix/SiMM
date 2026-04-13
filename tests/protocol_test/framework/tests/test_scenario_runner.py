"""Unit tests for scenario_runner.py — validation step parsing and registries."""

from framework.scenario_runner import (
    ScenarioRunner,
    ValidationStep,
    _FAULT_REGISTRY,
    _VALIDATION_REGISTRY,
    _parse_validation_steps,
    _resolve_target,
)


class TestParseValidationSteps:

    def test_empty(self):
        steps = _parse_validation_steps({})
        assert steps == []

    def test_single_step(self):
        data = {
            "validations": [
                {"type": "node_status", "target": "ds:0", "expected": "DEAD", "within_sec": 20}
            ]
        }
        steps = _parse_validation_steps(data)
        assert len(steps) == 1
        assert steps[0].type == "node_status"
        assert steps[0].target == "ds:0"
        assert steps[0].expected == "DEAD"
        assert steps[0].within_sec == 20

    def test_defaults(self):
        data = {
            "validations": [
                {"type": "shard_total", "expected": 64}
            ]
        }
        steps = _parse_validation_steps(data)
        assert steps[0].target == ""
        assert steps[0].field == ""
        assert steps[0].within_sec == 60
        assert steps[0].max_imbalance_ratio == 0.3

    def test_expected_converted_to_string(self):
        data = {
            "validations": [
                {"type": "shard_total", "expected": 64}
            ]
        }
        steps = _parse_validation_steps(data)
        assert steps[0].expected == "64"
        assert isinstance(steps[0].expected, str)

    def test_ds_status_with_field(self):
        data = {
            "validations": [
                {"type": "ds_status", "target": "ds:1", "field": "cm_ready", "expected": "true"}
            ]
        }
        steps = _parse_validation_steps(data)
        assert steps[0].field == "cm_ready"

    def test_shard_balance_with_ratio(self):
        data = {
            "validations": [
                {"type": "shard_balance", "max_imbalance_ratio": 0.5}
            ]
        }
        steps = _parse_validation_steps(data)
        assert steps[0].max_imbalance_ratio == 0.5

    def test_multiple_steps(self):
        data = {
            "validations": [
                {"type": "node_status", "target": "ds:0", "expected": "DEAD"},
                {"type": "shard_total", "expected": 64},
                {"type": "no_orphaned_shards"},
            ]
        }
        steps = _parse_validation_steps(data)
        assert len(steps) == 3
        assert [s.type for s in steps] == ["node_status", "shard_total", "no_orphaned_shards"]


class TestFaultRegistry:

    def test_builtin_fault_types(self):
        expected = {"sigkill", "sigterm", "sigstop", "sigcont", "kill_multiple", "restart_cm"}
        assert expected.issubset(set(_FAULT_REGISTRY.keys()))

    def test_all_entries_callable(self):
        for name, fn in _FAULT_REGISTRY.items():
            assert callable(fn), f"Fault '{name}' is not callable"


class TestValidationRegistry:

    def test_builtin_validation_types(self):
        expected = {
            "node_status", "alive_node_count", "all_nodes_running",
            "shard_total", "shard_balance", "no_orphaned_shards",
            "node_has_no_shards", "ds_status",
        }
        assert expected.issubset(set(_VALIDATION_REGISTRY.keys()))

    def test_all_entries_callable(self):
        for name, fn in _VALIDATION_REGISTRY.items():
            assert callable(fn), f"Validation '{name}' is not callable"


class TestScenarioRunnerLists:

    def test_list_fault_types(self):
        types = ScenarioRunner.list_fault_types()
        assert "sigkill" in types
        assert "restart_cm" in types

    def test_list_validation_types(self):
        types = ScenarioRunner.list_validation_types()
        assert "node_status" in types
        assert "shard_total" in types
