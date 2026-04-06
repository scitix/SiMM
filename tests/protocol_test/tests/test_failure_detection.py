"""Tests for CM failure detection of DS nodes.

Verifies the full failure detection pipeline:
  DS killed → CM heartbeat timeout → mark DEAD → shard rebalance away from dead node
"""

import time


class TestFailureDetection:
    """Verify CM detects DS failure and completes shard migration."""

    def test_single_ds_kill_detected_and_shards_migrated(self, cluster_small):
        """Kill one DS; CM marks DEAD; shards migrate to alive nodes."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        # Record state before kill
        shard_dist_before = cluster_small.observer.get_shard_distribution()
        total_shards_before = sum(shard_dist_before.values())
        ds0_shards_before = shard_dist_before.get(addr, 0)
        assert ds0_shards_before > 0, f"DS0 should have shards before kill, got {shard_dist_before}"

        # Kill DS
        cluster_small.fault_injector.kill_process(ds0)
        assert not cluster_small.process_manager.is_alive(ds0)

        # CM should detect failure within heartbeat_timeout + scan_interval
        timeout = (
            cluster_small.config.cm_heartbeat_timeout_inSecs
            + cluster_small.config.cm_heartbeat_bg_scan_interval_inSecs
            + 5
        )
        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=timeout
        ), f"CM did not mark {addr} as DEAD within {timeout}s"

        # After DEAD, shards must be migrated away
        assert cluster_small.observer.wait_for_rebalance_complete(
            timeout=15
        ), "Rebalance did not complete after DS marked DEAD"

        # Verify: dead node has zero shards
        cluster_small.observer.assert_node_has_no_shards(addr)

        # Verify: total shard count preserved (no loss, no duplication)
        cluster_small.observer.assert_total_shard_count(total_shards_before)

        # Verify: all shards now on alive nodes only
        cluster_small.observer.assert_no_orphaned_shards()

        # Verify: alive nodes picked up the orphaned shards
        shard_dist_after = cluster_small.observer.get_shard_distribution()
        alive_addrs = [
            ds.addr_str for ds in cluster_small.data_servers if ds.index != 0
        ]
        for alive_addr in alive_addrs:
            after = shard_dist_after.get(alive_addr, 0)
            before = shard_dist_before.get(alive_addr, 0)
            assert after >= before, (
                f"Alive node {alive_addr} should have gained shards: "
                f"before={before}, after={after}"
            )

    def test_detection_timing(self, cluster_small):
        """Failure should be detected within expected time bounds."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        kill_time = time.time()
        cluster_small.fault_injector.kill_process(ds0)

        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=60
        )
        detect_time = time.time()
        elapsed = detect_time - kill_time

        # Detection lower bound: at least heartbeat_timeout (CM must wait this long)
        min_expected = cluster_small.config.cm_heartbeat_timeout_inSecs
        assert elapsed >= min_expected - 1, (
            f"Detection too fast ({elapsed:.1f}s), expected >= {min_expected}s. "
            f"CM should not mark DEAD before heartbeat timeout expires."
        )

        # Detection upper bound: heartbeat_timeout + 2 * scan_interval
        max_expected = (
            cluster_small.config.cm_heartbeat_timeout_inSecs
            + 2 * cluster_small.config.cm_heartbeat_bg_scan_interval_inSecs
        )
        assert elapsed <= max_expected + 5, (
            f"Detection took {elapsed:.1f}s, expected <= {max_expected}s"
        )

    def test_remaining_nodes_unaffected(self, cluster_small):
        """After killing one DS, remaining DS stay RUNNING with correct shard counts."""
        ds0 = cluster_small.get_ds_handle(0)

        # Record each alive DS's shard count before kill
        shard_dist_before = cluster_small.observer.get_shard_distribution()

        cluster_small.fault_injector.kill_process(ds0)

        # Wait for full failure detection + rebalance
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 15
        cluster_small.observer.wait_for_node_status(ds0.addr_str, "DEAD", timeout=timeout)
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)

        # Remaining DS: process alive + status RUNNING
        for ds in cluster_small.data_servers:
            if ds.index == 0:
                continue
            assert cluster_small.process_manager.is_alive(ds), (
                f"DS[{ds.index}] should still be alive"
            )
            status = cluster_small.observer.get_node_status(ds.addr_str)
            assert status == "RUNNING", (
                f"DS[{ds.index}] status is {status}, expected RUNNING"
            )

        # Remaining DS should have MORE shards than before (they absorbed ds0's shards)
        shard_dist_after = cluster_small.observer.get_shard_distribution()
        for ds in cluster_small.data_servers:
            if ds.index == 0:
                continue
            before = shard_dist_before.get(ds.addr_str, 0)
            after = shard_dist_after.get(ds.addr_str, 0)
            assert after > before, (
                f"DS[{ds.index}] ({ds.addr_str}) should have gained shards: "
                f"before={before}, after={after}"
            )

