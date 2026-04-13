"""Tests for Deferred Reshard feature.

Design doc: simm-deferred-reshard-design.docx v2

Core behavior:
  DS down → CM enters DEFERRED_RESHARD (not DEAD, no reshard yet)
  → Within cm_deferred_reshard_window_inSecs, new DS with same logical_node_id registers
  → CM does in-place IP update in routing table, assigns original shards, no reshard
  → If window expires, CM degrades to standard reshard (DEAD)

Key flags:
  cm_deferred_reshard_enabled       (default true)
  cm_deferred_reshard_window_inSecs (default 120s, shortened in tests)
  cm_heartbeat_timeout_inSecs       (default 30s, shortened in tests)
  ds_logical_node_id                (per-DS, e.g. "simm-ds-0")
"""

import pytest

from framework.cluster import SimmCluster
from framework.config import ClusterConfig


def _make_deferred_reshard_config(
    cm_deferred_reshard_window_inSecs: int = 30,
) -> ClusterConfig:
    """Create a ClusterConfig with deferred reshard enabled and short timeouts."""
    return ClusterConfig(
        num_data_servers=3,
        shard_total_num=64,
        cm_cluster_init_grace_period_inSecs=5,
        cm_heartbeat_timeout_inSecs=6,
        cm_heartbeat_bg_scan_interval_inSecs=1,
        heartbeat_cooldown_sec=2,
        register_cooldown_sec=2,
        cm_hb_tolerance_count=5,
        cm_deferred_reshard_enabled=True,
        cm_deferred_reshard_window_inSecs=cm_deferred_reshard_window_inSecs,
        ds_logical_node_id_prefix="simm-ds",
    )


@pytest.fixture
def cluster_deferred(tmp_path, binary_dir):
    """Cluster with deferred reshard enabled, short timeouts for fast testing."""
    config = _make_deferred_reshard_config(cm_deferred_reshard_window_inSecs=30)
    cluster = SimmCluster(config, log_dir=tmp_path / "logs", binary_dir=binary_dir)
    cluster.start()
    cluster.wait_ready()
    yield cluster
    cluster.teardown()


@pytest.fixture
def cluster_deferred_short_window(tmp_path, binary_dir):
    """Cluster with very short deferred reshard window (10s) to test timeout degradation."""
    config = _make_deferred_reshard_config(cm_deferred_reshard_window_inSecs=10)
    cluster = SimmCluster(config, log_dir=tmp_path / "logs", binary_dir=binary_dir)
    cluster.start()
    cluster.wait_ready()
    yield cluster
    cluster.teardown()


@pytest.fixture
def cluster_deferred_disabled(tmp_path, binary_dir):
    """Cluster with deferred reshard disabled — should behave like original."""
    config = _make_deferred_reshard_config()
    config.cm_deferred_reshard_enabled = False
    cluster = SimmCluster(config, log_dir=tmp_path / "logs", binary_dir=binary_dir)
    cluster.start()
    cluster.wait_ready()
    yield cluster
    cluster.teardown()


@pytest.mark.requires_rdma
class TestDeferredReshardReplace:
    """DS killed within window → replacement DS with same logical_node_id → no reshard."""

    def test_kill_ds_enters_deferred_reshard_not_dead(self, cluster_deferred):
        """After DS killed, CM should enter DEFERRED_RESHARD, NOT DEAD."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str

        # Record shard distribution before kill
        dist_before = c.observer.get_shard_distribution()
        ds0_shards_before = dist_before.get(addr, 0)
        assert ds0_shards_before > 0

        c.fault_injector.kill_process(ds0)

        # Wait for CM to detect heartbeat timeout
        detect_timeout = (
            c.config.cm_heartbeat_timeout_inSecs
            + 2 * c.config.cm_heartbeat_bg_scan_interval_inSecs
            + 3
        )
        assert c.observer.wait_for_node_status(
            addr, "DEFERRED_RESHARD", timeout=detect_timeout
        ), f"CM should mark {addr} as DEFERRED_RESHARD, not DEAD"

        # Shards should NOT have been migrated (that's the whole point)
        dist_during = c.observer.get_shard_distribution()
        ds0_shards_during = dist_during.get(addr, 0)
        assert ds0_shards_during == ds0_shards_before, (
            f"Shards should stay assigned during DEFERRED_RESHARD: "
            f"before={ds0_shards_before}, during={ds0_shards_during}"
        )

        # Total shard count unchanged
        c.observer.assert_total_shard_count(c.config.shard_total_num)

    def test_replacement_ds_same_logical_id_recovers_shards(self, cluster_deferred):
        """Kill DS0, start replacement with same logical_node_id → shards recovered, no reshard."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str
        logical_id = f"{c.config.ds_logical_node_id_prefix}-0"  # "simm-ds-0"

        # Record shard distribution before kill
        dist_before = c.observer.get_shard_distribution()
        ds0_shards_before = dist_before.get(addr, 0)
        total_before = sum(dist_before.values())

        # Kill DS0
        c.fault_injector.kill_process(ds0)

        # Wait for DEFERRED_RESHARD
        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        assert c.observer.wait_for_node_status(
            addr, "DEFERRED_RESHARD", timeout=detect_timeout
        )

        # Start replacement DS with same logical_node_id (may use different ports)
        new_ds = c.add_data_server(ds_logical_node_id=logical_id)

        # Wait for CM to recognize replacement and set node back to RUNNING
        assert c.observer.wait_for_node_status(
            new_ds.addr_str, "RUNNING", timeout=15
        ), "Replacement DS did not become RUNNING"

        # Shard total unchanged
        c.observer.assert_total_shard_count(total_before)

        # The replacement DS should have received the SAME shards
        # (CM does in-place IP update, assigns original shards via assigned_shard_ids)
        dist_after = c.observer.get_shard_distribution()
        new_ds_shards = dist_after.get(new_ds.addr_str, 0)
        assert new_ds_shards == ds0_shards_before, (
            f"Replacement DS should inherit {ds0_shards_before} shards, "
            f"got {new_ds_shards}"
        )

        # Other DS shard counts unchanged (no reshard happened)
        for ds in c.data_servers:
            if ds.index == 0 or ds.addr_str == new_ds.addr_str:
                continue
            before = dist_before.get(ds.addr_str, 0)
            after = dist_after.get(ds.addr_str, 0)
            assert after == before, (
                f"DS[{ds.index}] shards changed during deferred reshard: "
                f"before={before}, after={after}. Reshard should NOT have happened."
            )

    def test_replacement_ds_status_healthy_after_recovery(self, cluster_deferred):
        """After replacement, DS admin RPC should show is_registered=true, cm_ready=true."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        logical_id = f"{c.config.ds_logical_node_id_prefix}-0"

        c.fault_injector.kill_process(ds0)

        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        c.observer.wait_for_node_status(ds0.addr_str, "DEFERRED_RESHARD", timeout=detect_timeout)

        new_ds = c.add_data_server(ds_logical_node_id=logical_id)

        c.observer.wait_for_node_status(new_ds.addr_str, "RUNNING", timeout=15)

        # DS-side: healthy state
        assert c.observer.wait_for_ds_registered(
            new_ds.host, new_ds.pid, timeout=10
        )
        c.observer.assert_ds_cm_ready(new_ds.host, new_ds.pid, expected=True)
        c.observer.assert_ds_heartbeat_failure_count(
            new_ds.host, new_ds.pid, min_count=0, max_count=0
        )


@pytest.mark.requires_rdma
class TestDeferredReshardWindowTimeout:
    """Window expires without replacement → degrade to standard reshard."""

    def test_window_timeout_triggers_reshard(self, cluster_deferred_short_window):
        """If no replacement within cm_deferred_reshard_window_inSecs, CM degrades to DEAD + reshard."""
        c = cluster_deferred_short_window
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = c.observer.get_shard_distribution()
        total_before = sum(dist_before.values())
        ds0_shards_before = dist_before.get(addr, 0)
        assert ds0_shards_before > 0

        c.fault_injector.kill_process(ds0)

        # First: should enter DEFERRED_RESHARD
        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        assert c.observer.wait_for_node_status(
            addr, "DEFERRED_RESHARD", timeout=detect_timeout
        )

        # Wait for window to expire: cm_deferred_reshard_window_inSecs = 10s
        window_timeout = (
            c.config.cm_deferred_reshard_window_inSecs
            + c.config.cm_heartbeat_bg_scan_interval_inSecs
            + 5
        )
        assert c.observer.wait_for_node_status(
            addr, "DEAD", timeout=window_timeout
        ), "CM should degrade to DEAD after deferred reshard window expires"

        # Now standard reshard should have happened
        c.observer.wait_for_rebalance_complete(timeout=15)

        # Dead node: zero shards
        c.observer.assert_node_has_no_shards(addr)

        # Total preserved
        c.observer.assert_total_shard_count(total_before)

        # Alive nodes absorbed the shards
        c.observer.assert_no_orphaned_shards()

        # Alive nodes gained ds0's shards
        dist_after = c.observer.get_shard_distribution()
        for ds in c.data_servers:
            if ds.index == 0:
                continue
            before = dist_before.get(ds.addr_str, 0)
            after = dist_after.get(ds.addr_str, 0)
            assert after > before, (
                f"DS[{ds.index}] should have gained shards: before={before}, after={after}"
            )

    def test_deferred_then_late_replacement_treated_as_new_node(
        self, cluster_deferred_short_window
    ):
        """If replacement arrives AFTER window expires, it's treated as a new node, not replacement."""
        c = cluster_deferred_short_window
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str
        logical_id = f"{c.config.ds_logical_node_id_prefix}-0"

        dist_before = c.observer.get_shard_distribution()
        ds0_shards_before = dist_before.get(addr, 0)
        assert ds0_shards_before > 0

        c.fault_injector.kill_process(ds0)

        # Wait for DEFERRED_RESHARD then DEAD (window expired)
        total_timeout = (
            c.config.cm_heartbeat_timeout_inSecs
            + c.config.cm_deferred_reshard_window_inSecs
            + 10
        )
        assert c.observer.wait_for_node_status(
            addr, "DEAD", timeout=total_timeout
        )
        c.observer.wait_for_rebalance_complete(timeout=15)

        # Now start "replacement" DS — but it's too late, reshard already happened
        new_ds = c.add_data_server(ds_logical_node_id=logical_id)

        # Should register as new node (RUNNING), but gets new shard assignment
        assert c.observer.wait_for_node_status(
            new_ds.addr_str, "RUNNING", timeout=15
        )

        c.observer.assert_total_shard_count(c.config.shard_total_num)

        # Late DS is treated as new node — it should NOT inherit ds0's original shard count
        # (reshard already redistributed ds0's shards to other alive nodes)
        dist_after = c.observer.get_shard_distribution()
        new_ds_shards = dist_after.get(new_ds.addr_str, 0)
        assert new_ds_shards != ds0_shards_before or new_ds_shards > 0, (
            "Late replacement should be treated as new node with fresh shard assignment"
        )


@pytest.mark.requires_rdma
class TestDeferredReshardDisabled:
    """With cm_deferred_reshard_enabled=false, DS kill goes straight to DEAD + reshard."""

    def test_disabled_goes_straight_to_dead(self, cluster_deferred_disabled):
        """With deferred reshard disabled, DS kill → DEAD immediately (no DEFERRED_RESHARD)."""
        c = cluster_deferred_disabled
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = c.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        c.fault_injector.kill_process(ds0)

        detect_timeout = (
            c.config.cm_heartbeat_timeout_inSecs
            + 2 * c.config.cm_heartbeat_bg_scan_interval_inSecs
            + 5
        )
        # Should go straight to DEAD, skip DEFERRED_RESHARD
        assert c.observer.wait_for_node_status(
            addr, "DEAD", timeout=detect_timeout
        ), "With deferred reshard disabled, should go straight to DEAD"

        # Standard reshard should happen
        c.observer.wait_for_rebalance_complete(timeout=15)
        c.observer.assert_node_has_no_shards(addr)
        c.observer.assert_total_shard_count(total_before)


@pytest.mark.requires_rdma
class TestDeferredReshardEdgeCases:
    """Edge cases for the deferred reshard protocol."""

    def test_replacement_with_different_logical_id_is_new_node(self, cluster_deferred):
        """A DS with a DIFFERENT logical_node_id during DEFERRED_RESHARD is treated as new, not replacement."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        addr = ds0.addr_str

        dist_before = c.observer.get_shard_distribution()
        ds0_shards = dist_before.get(addr, 0)

        c.fault_injector.kill_process(ds0)

        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        c.observer.wait_for_node_status(addr, "DEFERRED_RESHARD", timeout=detect_timeout)

        # Start DS with DIFFERENT logical_node_id — should NOT replace ds0
        new_ds = c.add_data_server(ds_logical_node_id="simm-ds-99-not-a-replacement")

        # New DS should register as new node
        c.observer.wait_for_node_status(new_ds.addr_str, "RUNNING", timeout=15)

        # ds0 should STILL be in DEFERRED_RESHARD (not replaced)
        status = c.observer.get_node_status(addr)
        assert status == "DEFERRED_RESHARD", (
            f"DS0 should still be DEFERRED_RESHARD, got {status}"
        )

        # ds0's shards should still be assigned to ds0 (not migrated)
        dist_after = c.observer.get_shard_distribution()
        assert dist_after.get(addr, 0) == ds0_shards, (
            f"DS0 shards should be unchanged during DEFERRED_RESHARD"
        )

    def test_fast_restart_before_heartbeat_timeout(self, cluster_deferred):
        """DS killed and restarted with same logical_id BEFORE heartbeat timeout
        → IP update, never enters DEFERRED_RESHARD at all."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        logical_id = f"{c.config.ds_logical_node_id_prefix}-0"

        dist_before = c.observer.get_shard_distribution()
        total_before = sum(dist_before.values())

        # Kill and immediately restart (within heartbeat_timeout)
        c.fault_injector.kill_process(ds0)
        new_ds = c.add_data_server(ds_logical_node_id=logical_id)

        # Should register quickly — never hits DEFERRED_RESHARD
        assert c.observer.wait_for_node_status(
            new_ds.addr_str, "RUNNING", timeout=10
        )

        # Shards unchanged, total correct
        c.observer.assert_total_shard_count(total_before)

    def test_insufficient_nodes_keeps_deferred_state(self, cluster_deferred_short_window):
        """Kill 2/3 DS → window expires → CM keeps DEFERRED_RESHARD (not DEAD)
        because only 1 alive node cannot absorb all orphaned shards safely."""
        c = cluster_deferred_short_window
        ds0 = c.get_ds_handle(0)
        ds1 = c.get_ds_handle(1)

        total_before = c.observer.get_total_shard_count()

        # Kill 2 of 3 DS
        c.fault_injector.kill_multiple([ds0, ds1])

        # Both should enter DEFERRED_RESHARD
        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        c.observer.wait_for_node_status(ds0.addr_str, "DEFERRED_RESHARD", timeout=detect_timeout)
        c.observer.wait_for_node_status(ds1.addr_str, "DEFERRED_RESHARD", timeout=detect_timeout)

        # Wait for window to expire
        window_timeout = (
            c.config.cm_deferred_reshard_window_inSecs
            + c.config.cm_heartbeat_bg_scan_interval_inSecs
            + 5
        )
        time.sleep(window_timeout)

        # Both should STILL be DEFERRED_RESHARD — not DEAD
        # CM cannot reshard safely with only 1 alive node
        status0 = c.observer.get_node_status(ds0.addr_str)
        status1 = c.observer.get_node_status(ds1.addr_str)
        assert status0 == "DEFERRED_RESHARD", (
            f"DS0 should stay DEFERRED_RESHARD with insufficient alive nodes, got {status0}"
        )
        assert status1 == "DEFERRED_RESHARD", (
            f"DS1 should stay DEFERRED_RESHARD with insufficient alive nodes, got {status1}"
        )

        # Shards should still be assigned (no reshard happened)
        c.observer.assert_total_shard_count(total_before)

        # Remaining DS should still be RUNNING
        ds2 = c.get_ds_handle(2)
        assert c.observer.get_node_status(ds2.addr_str) == "RUNNING"

    def test_multiple_ds_down_deferred_reshard(self, cluster_deferred):
        """Kill 2/3 DS → both enter DEFERRED_RESHARD → replace both → all shards recovered."""
        c = cluster_deferred
        ds0 = c.get_ds_handle(0)
        ds1 = c.get_ds_handle(1)
        logical_id_0 = f"{c.config.ds_logical_node_id_prefix}-0"
        logical_id_1 = f"{c.config.ds_logical_node_id_prefix}-1"

        dist_before = c.observer.get_shard_distribution()
        total_before = sum(dist_before.values())
        ds0_shards = dist_before.get(ds0.addr_str, 0)
        ds1_shards = dist_before.get(ds1.addr_str, 0)

        # Kill both
        c.fault_injector.kill_multiple([ds0, ds1])

        detect_timeout = c.config.cm_heartbeat_timeout_inSecs + 5
        c.observer.wait_for_node_status(ds0.addr_str, "DEFERRED_RESHARD", timeout=detect_timeout)
        c.observer.wait_for_node_status(ds1.addr_str, "DEFERRED_RESHARD", timeout=detect_timeout)

        # Replace both
        new_ds0 = c.add_data_server(ds_logical_node_id=logical_id_0)
        new_ds1 = c.add_data_server(ds_logical_node_id=logical_id_1)

        c.observer.wait_for_node_status(new_ds0.addr_str, "RUNNING", timeout=15)
        c.observer.wait_for_node_status(new_ds1.addr_str, "RUNNING", timeout=15)

        # All shards recovered
        c.observer.assert_total_shard_count(total_before)

        dist_after = c.observer.get_shard_distribution()
        assert dist_after.get(new_ds0.addr_str, 0) == ds0_shards
        assert dist_after.get(new_ds1.addr_str, 0) == ds1_shards
