"""Tests for simultaneous multiple node failures.

Verifies:
  - CM detects all dead nodes
  - Shards from ALL dead nodes migrate to alive nodes
  - Total shard count preserved
  - Alive nodes unaffected
"""

import pytest


class TestMultiFailure:
    """Verify cluster handles multiple simultaneous DS failures."""

    def test_kill_two_of_six(self, cluster_medium):
        """Kill 2/6 DS simultaneously → shards migrate to remaining 4."""
        ds0 = cluster_medium.get_ds_handle(0)
        ds1 = cluster_medium.get_ds_handle(1)

        # Record before state
        dist_before = cluster_medium.observer.get_shard_distribution()
        total_before = sum(dist_before.values())
        killed_shards = (
            dist_before.get(ds0.addr_str, 0) + dist_before.get(ds1.addr_str, 0)
        )
        assert killed_shards > 0, "Killed DS should have had shards"

        # Kill simultaneously
        cluster_medium.fault_injector.kill_multiple([ds0, ds1])

        timeout = cluster_medium.config.cm_heartbeat_timeout_inSecs + 15

        # Both should be detected as DEAD
        assert cluster_medium.observer.wait_for_node_status(
            ds0.addr_str, "DEAD", timeout=timeout
        ), f"DS0 {ds0.addr_str} not marked DEAD"
        assert cluster_medium.observer.wait_for_node_status(
            ds1.addr_str, "DEAD", timeout=timeout
        ), f"DS1 {ds1.addr_str} not marked DEAD"

        # Rebalance must complete
        assert cluster_medium.observer.wait_for_rebalance_complete(
            timeout=15
        ), "Rebalance did not complete"

        # Dead nodes: zero shards
        cluster_medium.observer.assert_node_has_no_shards(ds0.addr_str)
        cluster_medium.observer.assert_node_has_no_shards(ds1.addr_str)

        # Total preserved
        cluster_medium.observer.assert_total_shard_count(total_before)

        # Balance on remaining 4 nodes
        cluster_medium.observer.assert_shard_balance(max_imbalance_ratio=0.3)
        cluster_medium.observer.assert_no_orphaned_shards()

        # Alive nodes should all have gained shards
        dist_after = cluster_medium.observer.get_shard_distribution()
        alive_ds = [
            ds for ds in cluster_medium.data_servers
            if ds.index not in (0, 1)
        ]
        for ds in alive_ds:
            before = dist_before.get(ds.addr_str, 0)
            after = dist_after.get(ds.addr_str, 0)
            assert after >= before, (
                f"DS[{ds.index}] should have gained shards: "
                f"before={before}, after={after}"
            )

    def test_kill_two_of_six_sequential(self, cluster_medium):
        """Kill 2 DS one by one; verify intermediate and final shard state."""
        ds0 = cluster_medium.get_ds_handle(0)
        ds1 = cluster_medium.get_ds_handle(1)

        dist_before = cluster_medium.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        # Kill first DS
        cluster_medium.fault_injector.kill_process(ds0)

        timeout = cluster_medium.config.cm_heartbeat_timeout_inSecs + 15
        assert cluster_medium.observer.wait_for_node_status(
            ds0.addr_str, "DEAD", timeout=timeout
        )
        cluster_medium.observer.wait_for_rebalance_complete(timeout=15)

        # Intermediate check: ds0 has 0 shards, total preserved, 5 alive
        cluster_medium.observer.assert_node_has_no_shards(ds0.addr_str)
        cluster_medium.observer.assert_total_shard_count(total_before)
        assert cluster_medium.observer.get_alive_node_count() == 5

        # Kill second DS
        cluster_medium.fault_injector.kill_process(ds1)
        assert cluster_medium.observer.wait_for_node_status(
            ds1.addr_str, "DEAD", timeout=timeout
        )
        cluster_medium.observer.wait_for_rebalance_complete(timeout=15)

        # Final check
        cluster_medium.observer.assert_node_has_no_shards(ds1.addr_str)
        cluster_medium.observer.assert_total_shard_count(total_before)
        assert cluster_medium.observer.get_alive_node_count() == 4
        cluster_medium.observer.assert_no_orphaned_shards()
        cluster_medium.observer.assert_shard_balance(max_imbalance_ratio=0.3)
