"""Unit tests for config.py — YAML loading, deep merge, config parsing."""

import textwrap
from pathlib import Path

import pytest

from framework.config import (
    ClusterConfig,
    FaultConfig,
    HostConfig,
    deep_merge,
    dict_to_cluster_config,
    dict_to_fault_configs,
    load_yaml,
    merge_yaml_files,
)


class TestDeepMerge:

    def test_flat_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        base = {"x": {"a": 1, "b": 2}}
        override = {"x": {"b": 3, "c": 4}}
        result = deep_merge(base, override)
        assert result == {"x": {"a": 1, "b": 3, "c": 4}}

    def test_override_replaces_non_dict_with_dict(self):
        base = {"x": 1}
        override = {"x": {"nested": True}}
        result = deep_merge(base, override)
        assert result == {"x": {"nested": True}}

    def test_override_replaces_dict_with_non_dict(self):
        base = {"x": {"nested": True}}
        override = {"x": 42}
        result = deep_merge(base, override)
        assert result == {"x": 42}

    def test_does_not_mutate_inputs(self):
        base = {"x": {"a": 1}}
        override = {"x": {"b": 2}}
        base_copy = {"x": {"a": 1}}
        deep_merge(base, override)
        assert base == base_copy

    def test_empty_base(self):
        result = deep_merge({}, {"a": 1})
        assert result == {"a": 1}

    def test_empty_override(self):
        result = deep_merge({"a": 1}, {})
        assert result == {"a": 1}

    def test_both_empty(self):
        result = deep_merge({}, {})
        assert result == {}

    def test_three_level_nesting(self):
        base = {"a": {"b": {"c": 1, "d": 2}}}
        override = {"a": {"b": {"c": 99}}}
        result = deep_merge(base, override)
        assert result == {"a": {"b": {"c": 99, "d": 2}}}


class TestLoadYaml:

    def test_load_valid_yaml(self, tmp_path):
        f = tmp_path / "test.yaml"
        f.write_text("cluster:\n  shard_total_num: 128\n")
        data = load_yaml(f)
        assert data == {"cluster": {"shard_total_num": 128}}

    def test_load_empty_yaml(self, tmp_path):
        f = tmp_path / "empty.yaml"
        f.write_text("")
        data = load_yaml(f)
        assert data == {}

    def test_load_yaml_with_only_comment(self, tmp_path):
        f = tmp_path / "comment.yaml"
        f.write_text("# just a comment\n")
        data = load_yaml(f)
        assert data == {}


class TestMergeYamlFiles:

    def test_merge_two_files(self, tmp_path):
        f1 = tmp_path / "base.yaml"
        f1.write_text("cluster:\n  shard_total_num: 64\n  cluster_manager:\n    cm_heartbeat_timeout_inSecs: 30\n")
        f2 = tmp_path / "override.yaml"
        f2.write_text("cluster:\n  cluster_manager:\n    cm_heartbeat_timeout_inSecs: 6\n")
        result = merge_yaml_files(f1, f2)
        assert result["cluster"]["shard_total_num"] == 64
        assert result["cluster"]["cluster_manager"]["cm_heartbeat_timeout_inSecs"] == 6

    def test_merge_single_file(self, tmp_path):
        f = tmp_path / "only.yaml"
        f.write_text("key: value\n")
        result = merge_yaml_files(f)
        assert result == {"key": "value"}


class TestDictToClusterConfig:

    def test_minimal_dict(self):
        config = dict_to_cluster_config({})
        assert config.num_data_servers == 3
        assert config.shard_total_num == 64
        assert config.cm_heartbeat_timeout_inSecs == 10
        assert config.cm_host is None
        assert config.ds_hosts == []

    def test_full_dict(self):
        data = {
            "cluster": {
                "shard_total_num": 128,
                "build_mode": "debug",
                "cluster_manager": {
                    "cm_cluster_init_grace_period_inSecs": 15,
                    "cm_heartbeat_timeout_inSecs": 20,
                    "cm_heartbeat_bg_scan_interval_inSecs": 3,
                    "cm_deferred_reshard_enabled": False,
                    "cm_deferred_reshard_window_inSecs": 60,
                },
                "data_servers": {
                    "count": 6,
                    "heartbeat_cooldown_sec": 3,
                    "register_cooldown_sec": 5,
                    "cm_hb_tolerance_count": 10,
                    "cm_connect_retry_interval_sec": 2,
                    "memory_limit_bytes": 2 << 30,
                    "ds_logical_node_id_prefix": "test-ds",
                },
                "ssh": {
                    "user": "admin",
                    "port": 2222,
                },
            }
        }
        config = dict_to_cluster_config(data)
        assert config.num_data_servers == 6
        assert config.shard_total_num == 128
        assert config.build_mode == "debug"
        assert config.cm_cluster_init_grace_period_inSecs == 15
        assert config.cm_heartbeat_timeout_inSecs == 20
        assert config.cm_heartbeat_bg_scan_interval_inSecs == 3
        assert config.cm_deferred_reshard_enabled is False
        assert config.cm_deferred_reshard_window_inSecs == 60
        assert config.heartbeat_cooldown_sec == 3
        assert config.register_cooldown_sec == 5
        assert config.cm_hb_tolerance_count == 10
        assert config.cm_connect_retry_interval_sec == 2
        assert config.memory_limit_bytes == 2 << 30
        assert config.ds_logical_node_id_prefix == "test-ds"
        assert config.ssh_user == "admin"
        assert config.ssh_port == 2222

    def test_with_host_topology(self):
        data = {
            "cluster": {
                "hosts": {
                    "cm": {"ip": "10.0.0.1", "binary_dir": "/opt/bin"},
                    "ds": [
                        {"ip": "10.0.0.2"},
                        {"ip": "10.0.0.3", "log_dir": "/var/log"},
                    ],
                },
            }
        }
        config = dict_to_cluster_config(data)
        assert config.cm_host is not None
        assert config.cm_host.ip == "10.0.0.1"
        assert config.cm_host.binary_dir == "/opt/bin"
        assert len(config.ds_hosts) == 2
        assert config.ds_hosts[0].ip == "10.0.0.2"
        assert config.ds_hosts[0].log_dir == "/tmp/simm_test_logs"  # default
        assert config.ds_hosts[1].log_dir == "/var/log"


class TestDictToFaultConfigs:

    def test_empty(self):
        assert dict_to_fault_configs({}) == []

    def test_single_fault_with_target(self):
        data = {
            "faults": [
                {"type": "sigkill", "target": "ds:0", "delay_after_ready_sec": 5}
            ]
        }
        faults = dict_to_fault_configs(data)
        assert len(faults) == 1
        assert faults[0].fault_type == "sigkill"
        assert faults[0].targets == ["ds:0"]
        assert faults[0].delay_after_sec == 5
        assert faults[0].duration_sec is None

    def test_fault_with_targets_list(self):
        data = {
            "faults": [
                {"type": "kill_multiple", "targets": ["ds:0", "ds:1"]}
            ]
        }
        faults = dict_to_fault_configs(data)
        assert faults[0].targets == ["ds:0", "ds:1"]

    def test_fault_with_duration(self):
        data = {
            "faults": [
                {"type": "sigstop", "target": "ds:0", "duration_sec": 15}
            ]
        }
        faults = dict_to_fault_configs(data)
        assert faults[0].duration_sec == 15

    def test_multiple_faults(self):
        data = {
            "faults": [
                {"type": "sigkill", "target": "ds:0"},
                {"type": "restart_cm", "target": "cm", "duration_sec": 3},
            ]
        }
        faults = dict_to_fault_configs(data)
        assert len(faults) == 2
        assert faults[0].fault_type == "sigkill"
        assert faults[1].fault_type == "restart_cm"


class TestClusterConfigProperties:

    def test_cm_failure_detection_max_sec(self):
        config = ClusterConfig(
            cm_heartbeat_timeout_inSecs=10,
            cm_heartbeat_bg_scan_interval_inSecs=2,
        )
        assert config.cm_failure_detection_max_sec == 14  # 10 + 2*2

    def test_ds_cm_failure_detection_sec(self):
        config = ClusterConfig(
            cm_hb_tolerance_count=5,
            heartbeat_cooldown_sec=2,
        )
        assert config.ds_cm_failure_detection_sec == 10  # 5 * 2

    def test_ds_rejoin_max_sec(self):
        config = ClusterConfig(
            cm_hb_tolerance_count=5,
            heartbeat_cooldown_sec=2,
            register_cooldown_sec=2,
        )
        # ds_cm_failure_detection_sec(10) + register_cooldown_sec(2) + 15
        assert config.ds_rejoin_max_sec == 27
