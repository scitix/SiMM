"""Tests for graceful shutdown (SIGTERM) handling.

Verifies:
  - DS exits cleanly on SIGTERM
  - CM detects absence via HB timeout
  - Shards migrate away from stopped node
  - CM itself shuts down cleanly on SIGTERM
"""

import time


class TestGracefulShutdown:
    """Verify SIGTERM triggers graceful shutdown with full shard migration."""

    def test_sigterm_ds_detected_and_shards_migrated(self, cluster_small):
        """SIGTERM to DS → CM detects DEAD → shards migrate."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        # Graceful stop
        cluster_small.fault_injector.graceful_stop(ds0)
        assert not cluster_small.process_manager.is_alive(ds0), (
            "DS should have exited after SIGTERM"
        )

        # CM detects via heartbeat timeout
        timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
        assert cluster_small.observer.wait_for_node_status(
            addr, "DEAD", timeout=timeout
        ), f"CM did not mark {addr} as DEAD after graceful shutdown"

        # Shards should migrate
        cluster_small.observer.wait_for_rebalance_complete(timeout=15)
        cluster_small.observer.assert_node_has_no_shards(addr)
        cluster_small.observer.assert_total_shard_count(total_before)
        cluster_small.observer.assert_no_orphaned_shards()

    def test_sigterm_cm_graceful(self, cluster_small):
        """SIGTERM to CM should trigger clean shutdown."""
        cluster_small.fault_injector.graceful_stop(cluster_small.cm)
        assert not cluster_small.process_manager.is_alive(cluster_small.cm), (
            "CM should have exited after SIGTERM"
        )

        # All DS should still be alive (DS survives CM shutdown)
        for ds in cluster_small.data_servers:
            assert cluster_small.process_manager.is_alive(ds), (
                f"DS[{ds.index}] should survive CM shutdown"
            )
