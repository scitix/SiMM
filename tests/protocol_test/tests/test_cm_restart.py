"""Tests for Cluster Manager crash and restart recovery.

Verifies the full CM restart protocol from BOTH sides:
  CM killed → DS detects HB failures (heartbeat_failure_count reaches cm_hb_tolerance_count)
  → DS sets cm_ready=false → CM restarts → DS calls RegisterToRestartedManager
  → DS sets cm_ready=true, is_registered=true, heartbeat_failure_count=0
  → CM rebuilds routing table from DS-reported shard lists
"""

import time

import pytest


@pytest.mark.requires_rdma
class TestCMRestart:
    """Verify DS re-register after CM crash, with DS-side and shard verification."""

    def test_cm_crash_all_ds_rejoin(self, cluster_small):
        """Kill CM → DS detect failure → restart CM → all DS re-register."""

        # Kill CM
        cluster_small.fault_injector.kill_process(cluster_small.cm)
        assert not cluster_small.process_manager.is_alive(cluster_small.cm)

        # All DS should still be alive (DS doesn't exit when CM dies)
        for ds in cluster_small.data_servers:
            assert cluster_small.process_manager.is_alive(ds), (
                f"DS[{ds.index}] should survive CM crash"
            )

        # Wait for DS to detect CM failure via heartbeat_failure_count
        hb_failure_wait = (
            5 * cluster_small.config.heartbeat_cooldown_sec + 2
        )
        time.sleep(hb_failure_wait)

        # Restart CM with same ports
        new_cm = cluster_small.restart_cm()
        assert cluster_small.process_manager.is_alive(new_cm)

        # Wait for DS re-registration
        timeout = cluster_small.config.cm_cluster_init_grace_period_inSecs + 20
        assert cluster_small.observer.wait_for_node_count(
            cluster_small.config.num_data_servers, timeout=timeout
        ), "Not all DS re-registered after CM restart"

        # CM side: all nodes RUNNING
        cluster_small.observer.assert_all_nodes_running()

        # Shard routing table fully rebuilt
        cluster_small.observer.assert_total_shard_count(shard_total_before)

    def test_ds_detects_cm_failure_via_status(self, cluster_small):
        """After CM crash, DS admin RPC should show cm_ready=false, failure_count >= tolerance."""
        cluster_small.fault_injector.kill_process(cluster_small.cm)

        # Wait for DS to accumulate heartbeat failures
        # DS: cm_hb_tolerance_count(5) * heartbeat_cooldown_sec(2) = 10s
        hb_failure_wait = (
            5 * cluster_small.config.heartbeat_cooldown_sec + 5
        )

        # Each DS should detect CM failure: cm_ready becomes false
        for ds in cluster_small.data_servers:
            assert cluster_small.observer.wait_for_ds_cm_not_ready(
                ds.host, ds.pid, timeout=hb_failure_wait
            ), f"DS[{ds.index}] did not detect CM failure (cm_ready still true)"

        # Verify heartbeat_failure_count is non-zero on each DS
        for ds in cluster_small.data_servers:
            cluster_small.observer.assert_ds_heartbeat_failure_count(
                ds.host, ds.pid, min_count=5
            )

        # Restart CM so teardown works
        cluster_small.restart_cm()
        timeout = cluster_small.config.cm_cluster_init_grace_period_inSecs + 20
        cluster_small.observer.wait_for_node_count(
            cluster_small.config.num_data_servers, timeout=timeout
        )

    def test_ds_re_registration_resets_status(self, cluster_small):
        """After CM restart, DS status should show is_registered=true, cm_ready=true, failure_count=0."""
        cluster_small.fault_injector.kill_process(cluster_small.cm)

        hb_failure_wait = (
            5 * cluster_small.config.heartbeat_cooldown_sec + 2
        )
        time.sleep(hb_failure_wait)

        cluster_small.restart_cm()

        timeout = cluster_small.config.cm_cluster_init_grace_period_inSecs + 20
        assert cluster_small.observer.wait_for_node_count(
            cluster_small.config.num_data_servers, timeout=timeout
        )

        # Each DS should now report: registered=true, cm_ready=true, failure_count=0
        for ds in cluster_small.data_servers:
            assert cluster_small.observer.wait_for_ds_registered(
                ds.host, ds.pid, timeout=10
            ), f"DS[{ds.index}] did not re-register properly"

            cluster_small.observer.assert_ds_is_registered(ds.host, ds.pid)
            cluster_small.observer.assert_ds_cm_ready(ds.host, ds.pid, expected=True)
            cluster_small.observer.assert_ds_heartbeat_failure_count(
                ds.host, ds.pid, min_count=0, max_count=0
            )

    def test_shard_table_rebuilt_after_cm_restart(self, cluster_small):
        """After CM restart, shard routing table rebuilt from DS-reported shard lists."""
        shard_total_before = cluster_small.observer.get_total_shard_count()
        dist_before = cluster_small.observer.get_shard_distribution()

        cluster_small.fault_injector.kill_process(cluster_small.cm)
        time.sleep(5 * cluster_small.config.heartbeat_cooldown_sec + 2)
        cluster_small.restart_cm()

        timeout = cluster_small.config.cm_cluster_init_grace_period_inSecs + 20
        assert cluster_small.observer.wait_for_node_count(
            cluster_small.config.num_data_servers, timeout=timeout
        )

        # Shard table must be complete
        cluster_small.observer.assert_total_shard_count(shard_total_before)

        # Distribution should match pre-crash state
        dist_after = cluster_small.observer.get_shard_distribution()
        for addr, count_before in dist_before.items():
            count_after = dist_after.get(addr, 0)
            assert count_after == count_before, (
                f"Node {addr} shard count changed after CM restart: "
                f"before={count_before}, after={count_after}"
            )

