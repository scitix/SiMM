"""Tests for network partition scenarios.

NOTE: SiCL uses RDMA transport (InfiniBand/RoCEv2), NOT TCP/IP.
iptables-based partition does NOT work. The actual partition injection method
will be provided separately and plugged into FaultInjector.

These tests are currently skipped pending the RDMA-aware partition method.
"""

import pytest


@pytest.mark.skip(reason="SiCL uses RDMA transport; iptables partition not applicable. "
                         "Awaiting RDMA-aware partition injection method.")
class TestNetworkPartition:
    """Verify cluster handles network partitions correctly.

    Expected behavior:
      - Partition DS from CM → CM marks DS as DEAD, shards migrate
      - DS side: heartbeat_failure_count accumulates, cm_ready becomes false
      - Heal partition → DS re-registers, shards may be re-assigned
    """

    def test_partition_and_heal(self, cluster_small):
        """Partition DS0 from CM → DEAD + shard migration → heal → DS re-registers."""
        ds0 = cluster_small.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        with cluster_small.fault_injector.temporary_partition(
            ds0, [cluster_small.cm]
        ):
            # CM should detect DS0 as DEAD
            timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 10
            assert cluster_small.observer.wait_for_node_status(
                addr, "DEAD", timeout=timeout
            ), f"CM did not mark partitioned DS {addr} as DEAD"

            # Shards must migrate away from partitioned node
            cluster_small.observer.wait_for_rebalance_complete(timeout=15)
            cluster_small.observer.assert_node_has_no_shards(addr)
            cluster_small.observer.assert_total_shard_count(total_before)

        # After partition heals, DS should re-register
        rejoin_timeout = (
            5 * cluster_small.config.heartbeat_cooldown_sec
            + cluster_small.config.register_cooldown_sec
            + 15
        )
        assert cluster_small.observer.wait_for_node_status(
            addr, "RUNNING", timeout=rejoin_timeout
        ), f"DS {addr} did not rejoin after partition healed"

        # Shard total still correct
        cluster_small.observer.assert_total_shard_count(total_before)

    def test_partition_shard_rebalance(self, cluster_small):
        """During partition, shards rebalanced to alive nodes, total preserved."""
        ds0 = cluster_small.get_ds_handle(0)

        dist_before = cluster_small.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        with cluster_small.fault_injector.temporary_partition(
            ds0, [cluster_small.cm]
        ):
            timeout = cluster_small.config.cm_heartbeat_timeout_inSecs + 15
            cluster_small.observer.wait_for_rebalance_complete(timeout=timeout)
            cluster_small.observer.assert_no_orphaned_shards()
            cluster_small.observer.assert_total_shard_count(total_before)

            # Alive nodes should have absorbed the shards
            dist_during = cluster_small.observer.get_shard_distribution()
            for ds in cluster_small.data_servers:
                if ds.index == 0:
                    continue
                before = dist_before.get(ds.addr_str, 0)
                during = dist_during.get(ds.addr_str, 0)
                assert during >= before, (
                    f"DS[{ds.index}] should have gained shards during partition"
                )
