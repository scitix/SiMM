"""Tests for runtime flag management via admin interface."""

import pytest


@pytest.mark.requires_rdma
class TestFlagManagement:
    """Verify runtime gflag get/set/list via admin UDS."""

    def test_runtime_flag_change(self, cluster_small):
        """Change heartbeat timeout at runtime via admin, verify new value."""
        admin = cluster_small.observer.admin_client
        cm = cluster_small.cm

        # Set flag
        success = admin.set_flag(
            cm.host, cm.pid,
            "cm_heartbeat_timeout_inSecs", "15"
        )
        assert success, "Failed to set flag"

        # Verify
        val = admin.get_flag(
            cm.host, cm.pid,
            "cm_heartbeat_timeout_inSecs"
        )
        assert val == "15", f"Expected flag value '15', got '{val}'"

    def test_list_flags(self, cluster_small):
        """List all flags from CM and verify key flags exist."""
        admin = cluster_small.observer.admin_client
        cm = cluster_small.cm

        flags = admin.list_flags(cm.host, cm.pid)
        assert len(flags) > 0, "No flags returned"

        # Check some expected flags exist
        expected_flags = [
            "cm_heartbeat_timeout_inSecs",
            "cm_heartbeat_bg_scan_interval_inSecs",
            "shard_total_num",
        ]
        for flag_name in expected_flags:
            assert flag_name in flags, f"Expected flag '{flag_name}' not found"

    def test_set_ds_flag(self, cluster_small):
        """Set a flag on a data server."""
        admin = cluster_small.observer.admin_client
        ds0 = cluster_small.get_ds_handle(0)

        success = admin.set_flag(
            ds0.host, ds0.pid,
            "heartbeat_cooldown_sec", "3"
        )
        assert success, "Failed to set DS flag"

        val = admin.get_flag(
            ds0.host, ds0.pid,
            "heartbeat_cooldown_sec"
        )
        assert val == "3", f"Expected '3', got '{val}'"
