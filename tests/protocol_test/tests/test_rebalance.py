"""Tests for shard rebalancing after node failure.

Verifies:
  - Orphaned shards are migrated away from dead node
  - Total shard count is preserved (no loss, no duplication)
  - Post-rebalance distribution is balanced across alive nodes
  - Rebalance happens promptly after DEAD detection
"""

import time


class TestRebalance:
    """Verify shard rebalancing correctness after DS failure."""

    def test_shards_migrated_from_dead_node(self, cluster_small):
        """Kill DS0 → dead node must have 0 shards, all moved to alive nodes."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        # Record before state
        dist_before = cluster_small.observer.get_shard_distribution()
        ds0_shards_before = dist_before.get(addr, 0)
        assert ds0_shards_before > 0, f"DS0 has no shards to migrate: {dist_before}"

        cluster_small.fault_injector.kill_process(ds0)

        # Wait for detection + rebalance
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 15
        cluster_small.observer.wait_for_node_status(addr, "DEAD", timeout=timeout)
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)

        # Dead node: zero shards
        cluster_small.observer.assert_node_has_no_shards(addr)

        # Alive nodes absorbed the orphaned shards
        dist_after = cluster_small.observer.get_shard_distribution()
        for ds in cluster_small.data_servers:
            if ds.index == 0:
                continue
            before = dist_before.get(ds.addr_str, 0)
            after = dist_after.get(ds.addr_str, 0)
            assert after > before, (
                f"DS[{ds.index}] ({ds.addr_str}) didn't gain shards: "
                f"before={before}, after={after}"
            )

    def test_shard_total_preserved(self, cluster_small):
        """Rebalance must not lose or duplicate shards."""
        total_before = cluster_small.observer.get_total_shard_count()
        assert total_before == cluster_small.config.shard_total_num

        ds0 = cluster_small.get_ds_handle(0)
        cluster_small.fault_injector.kill_process(ds0)

        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 15
        cluster_small.observer.wait_for_node_status(ds0.addr_str, "DEAD", timeout=timeout)
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)

        cluster_small.observer.assert_total_shard_count(total_before)

    def test_shard_balance_after_rebalance(self, cluster_small):
        """After rebalance, shards balanced across remaining alive nodes."""
        ds0 = cluster_small.get_ds_handle(0)
        cluster_small.fault_injector.kill_process(ds0)

        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 15
        cluster_small.observer.wait_for_node_status(ds0.addr_str, "DEAD", timeout=timeout)
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)

        # 3 DS, 1 killed → 64 shards on 2 nodes → ~32 each
        cluster_small.observer.assert_shard_balance(max_imbalance_ratio=0.3)

        # Verify: each alive node has roughly total/alive shards
        dist = cluster_small.observer.get_shard_distribution()
        alive_count = sum(1 for ds in cluster_small.data_servers if ds.index != 0)
        expected_per_node = cluster_small.config.shard_total_num / alive_count
        for ds in cluster_small.data_servers:
            if ds.index == 0:
                continue
            node_shards = dist.get(ds.addr_str, 0)
            assert node_shards > 0, (
                f"DS[{ds.index}] has 0 shards after rebalance"
            )
            # Allow ±30% deviation from ideal
            assert abs(node_shards - expected_per_node) / expected_per_node <= 0.3, (
                f"DS[{ds.index}] has {node_shards} shards, "
                f"expected ~{expected_per_node:.0f}"
            )

    def test_rebalance_timing(self, cluster_small):
        """Rebalance should start immediately after DEAD detection, not delayed."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        cluster_small.fault_injector.kill_process(ds0)

        # Wait for DEAD and record the timestamp
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=timeout
        )
        dead_time = time.time()

        # Rebalance should complete quickly after DEAD (routing table update is synchronous)
        assert cluster_small.observer.wait_for_rebalance_complete(
            timeout=10
        ), "Rebalance did not complete within 10s of DEAD detection"
        rebalance_done_time = time.time()

        # Shards should have moved within a few seconds of DEAD
        elapsed = rebalance_done_time - dead_time
        assert elapsed < 10, (
            f"Rebalance took {elapsed:.1f}s after DEAD detection, expected < 10s"
        )

        # Verify the end state
        cluster_small.observer.assert_node_has_no_shards(addr)
        cluster_small.observer.assert_total_shard_count(
            cluster_small.config.shard_total_num
        )

