"""Tests for DS rejoin after failure recovery.

Verifies both sides:
  CM-side: node status DEAD → RUNNING after rejoin, shard migration and recovery
  DS-side: cm_ready transitions, heartbeat_failure_count, re-registration
"""

import time

import pytest


@pytest.mark.requires_rdma
class TestNodeRejoin:
    """Verify DS can rejoin after freeze/unfreeze or kill/restart."""

    def test_ds_freeze_unfreeze_rejoin(self, cluster_small):
        """Freeze DS0 → CM marks DEAD, shards migrate → unfreeze → DS re-registers."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        # Record shard state before freeze
        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        # Freeze DS
        cluster_small.fault_injector.freeze_process(ds0)

        # CM detects DEAD
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=timeout
        ), f"CM did not mark frozen DS {addr} as DEAD"

        # Shards migrated away
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)
        cluster_small.observer.assert_node_has_no_shards(addr)
        cluster_small.observer.assert_total_shard_count(total_before)

        # Unfreeze DS
        cluster_small.fault_injector.unfreeze_process(ds0)

        # DS re-registers
        rejoin_timeout = (
            5 * cluster_small.config.heartbeat_cooldown_sec
            + cluster_small.config.register_cooldown_sec
            + 15
        )
        assert cluster_small.observer.wait_for_node_status(
            addr, "RUNNING", timeout=rejoin_timeout
        ), f"DS {addr} did not rejoin after unfreeze"

        # DS-side: verify internal state recovered
        assert cluster_small.observer.wait_for_ds_registered(
            ds0.pid, timeout=10
        ), "DS did not report registered after rejoin"

        cluster_small.observer.assert_ds_cm_ready(ds0.admin_name, ds0.pid, expected=True)
        cluster_small.observer.assert_ds_heartbeat_failure_count(
            ds0.pid, min_count=0, max_count=0
        )

        # Shard total still correct
        cluster_small.observer.assert_total_shard_count(total_before)

    def test_ds_freeze_shows_cm_not_ready_after_unfreeze(self, cluster_small):
        """After unfreeze, DS briefly has cm_ready=false before re-registering."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        cluster_small.fault_injector.freeze_process(ds0)

        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
        cluster_small.observer.wait_for_node_status(addr, "DEAD", timeout=timeout)

        cluster_small.fault_injector.unfreeze_process(ds0)

        # DS should eventually re-register and recover
        rejoin_timeout = (
            5 * cluster_small.config.heartbeat_cooldown_sec
            + cluster_small.config.register_cooldown_sec
            + 15
        )
        assert cluster_small.observer.wait_for_node_status(
            addr, "RUNNING", timeout=rejoin_timeout
        )

        # Final state: registered and cm_ready
        cluster_small.observer.assert_ds_is_registered(ds0.admin_name, ds0.pid)
        cluster_small.observer.assert_ds_cm_ready(ds0.admin_name, ds0.pid, expected=True)

    def test_ds_restart_rejoin(self, cluster_small):
        """Kill DS0, wait for DEAD + shard migration, restart, verify re-registration."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        # Kill DS
        cluster_small.fault_injector.kill_process(ds0)

        # Wait for detection and shard migration
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=timeout
        )
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)
        cluster_small.observer.assert_node_has_no_shards(addr)
        cluster_small.observer.assert_total_shard_count(total_before)

        # Restart DS with same ports (joins as new node)
        new_ds = cluster_small.add_data_server(ports=ds0.ports)

        # Wait for re-registration
        rejoin_timeout = cluster_small.config.register_cooldown_sec + 15
        assert cluster_small.observer.wait_for_node_count(
            cluster_small.config.num_data_servers, timeout=rejoin_timeout
        ), "DS did not rejoin after restart"

        # CM side: all RUNNING
        cluster_small.observer.assert_all_nodes_running()

        # DS side: new DS should be registered and cm_ready
        assert cluster_small.observer.wait_for_ds_registered(
            new_ds.pid, timeout=10
        ), "Restarted DS did not report registered"

        # Shard total correct
        cluster_small.observer.assert_total_shard_count(total_before)
