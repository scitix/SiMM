"""Tests for node registration and initial cluster formation."""

import time


class TestNodeJoin:
    """Verify DS nodes register correctly during grace period."""

    def test_all_nodes_register(self, cluster_small):
        """After grace period, all DS should be RUNNING."""
        statuses = cluster_small.observer.get_all_node_statuses()
        assert len(statuses) >= cluster_small.config.num_data_servers, (
            f"Expected {cluster_small.config.num_data_servers} nodes, "
            f"got {len(statuses)}: {statuses}"
        )
        for addr, status in statuses.items():
            assert status == "RUNNING", f"Node {addr} is {status}, expected RUNNING"

    def test_initial_shard_distribution(self, cluster_small):
        """Shards should be roughly evenly distributed across DS after initial setup."""
        cluster_small.observer.assert_total_shard_count(
            cluster_small.config.shard_total_num
        )
        cluster_small.observer.assert_shard_balance(max_imbalance_ratio=0.1)

    def test_cm_log_shows_handshakes(self, cluster_small):
        """CM log should contain handshake events for each DS."""
        from framework.log_parser import LogParser
        from framework.ssh_executor import SshExecutor
        cm_log = LogParser(cluster_small.cm.log_path, cluster_small.cm.host, SshExecutor())
        events = cm_log.find_handshake_events()
        assert len(events) >= cluster_small.config.num_data_servers, (
            f"Expected at least {cluster_small.config.num_data_servers} handshake events, "
            f"found {len(events)}"
        )

    def test_ds_processes_alive(self, cluster_small):
        """All DS processes should be running."""
        for ds in cluster_small.data_servers:
            assert cluster_small.process_manager.is_alive(ds), (
                f"DS[{ds.index}] (pid={ds.pid}) is not alive"
            )

    def test_cm_process_alive(self, cluster_small):
        """CM process should be running."""
        assert cluster_small.process_manager.is_alive(cluster_small.cm), (
            f"CM (pid={cluster_small.cm.pid}) is not alive"
        )
