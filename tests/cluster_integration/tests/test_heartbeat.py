"""Tests for heartbeat monitoring — steady state verification on both CM and DS sides."""

import time

import pytest


class TestHeartbeat:
    """Verify heartbeat keeps nodes alive and shard distribution stable."""

    def test_heartbeat_keeps_alive(self, cluster_small):
        """After multiple heartbeat cycles, all nodes RUNNING with shards intact."""
        hb_interval = cluster_small.config.heartbeat_cooldown_sec
        time.sleep(hb_interval * 5)

        # CM side: all nodes RUNNING
        cluster_small.observer.assert_all_nodes_running()

        # Shard side: distribution unchanged, total correct
        cluster_small.observer.assert_total_shard_count(
            cluster_small.config.shard_total_num
        )
        cluster_small.observer.assert_shard_balance(max_imbalance_ratio=0.1)

    def test_stable_shard_distribution(self, cluster_small):
        """Shard distribution should not change during normal operation."""
        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        time.sleep(10)

        dist_after = cluster_small.observer.get_shard_distribution()
        total_after = sum(dist_after.values())

        assert total_before == total_after, (
            f"Shard total changed: {total_before} -> {total_after}"
        )
        for addr in dist_before:
            assert dist_before[addr] == dist_after.get(addr, 0), (
                f"Node {addr} shard count changed: "
                f"{dist_before[addr]} -> {dist_after.get(addr, 0)}"
            )

    @pytest.mark.requires_rdma
    def test_ds_status_healthy(self, cluster_small):
        """Under normal conditions, all DS report is_registered=true, cm_ready=true, failure_count=0."""
        hb_interval = cluster_small.config.heartbeat_cooldown_sec
        time.sleep(hb_interval * 5)

        for ds in cluster_small.data_servers:
            cluster_small.observer.assert_ds_is_registered(ds.pid)
            cluster_small.observer.assert_ds_cm_ready(ds.pid, expected=True)
            cluster_small.observer.assert_ds_heartbeat_failure_count(
                ds.pid, min_count=0, max_count=0
            )

    def test_stable_cluster_state(self, cluster_small):
        """Cluster state should remain stable (no status flapping) for 10 seconds."""
        assert cluster_small.observer.wait_for_stable_state(
            duration=10, check_interval=2
        ), "Cluster state was not stable for 10 seconds"
