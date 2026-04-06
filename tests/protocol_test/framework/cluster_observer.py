"""Cluster state observation, condition waiting, and invariant assertions."""

import logging
import math
import time

from .admin_client import AdminClient, AdminClientError, NodeInfo
from .log_parser import LogParser
from .process_manager import ProcessHandle

logger = logging.getLogger(__name__)


class ClusterObserver:
    """
    Queries cluster state and waits for conditions.
    Uses AdminClient (UDS-based) as primary path, falls back to LogParser.
    """

    def __init__(
        self,
        admin_client: AdminClient,
        cm_handle: ProcessHandle,
        cm_log_parser: LogParser,
        ds_log_parsers: dict[str, LogParser] | None = None,
    ):
        self._admin = admin_client
        self._cm = cm_handle
        self._cm_log = cm_log_parser
        self._ds_logs = ds_log_parsers or {}

    @property
    def admin_client(self) -> AdminClient:
        return self._admin

    # --- State queries via admin RPC ---

    def _list_nodes(self) -> list[NodeInfo]:
        try:
            return self._admin.list_nodes(self._cm.host, self._cm.pid)
        except AdminClientError:
            return []

    def get_alive_node_count(self) -> int:
        nodes = self._list_nodes()
        return sum(1 for n in nodes if n.status == "RUNNING")

    def get_dead_node_count(self) -> int:
        nodes = self._list_nodes()
        return sum(1 for n in nodes if n.status == "DEAD")

    def get_all_node_statuses(self) -> dict[str, str]:
        """Returns {node_addr: status}."""
        nodes = self._list_nodes()
        return {n.address: n.status for n in nodes}

    def get_node_status(self, node_addr: str) -> str | None:
        statuses = self.get_all_node_statuses()
        return statuses.get(node_addr)

    def get_shard_distribution(self) -> dict[str, int]:
        """Returns {node_addr: shard_count}."""
        try:
            return self._admin.list_shards(self._cm.host, self._cm.pid)
        except AdminClientError:
            return {}

    def get_total_shard_count(self) -> int:
        dist = self.get_shard_distribution()
        return sum(dist.values())

    # --- Condition waiters (polling) ---

    def wait_for_node_count(
        self, expected: int, alive_only: bool = True,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until the node count matches expected."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            count = self.get_alive_node_count() if alive_only else len(self._list_nodes())
            if count == expected:
                logger.info("Node count reached %d", expected)
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for node count=%d (current=%d)",
                       expected, self.get_alive_node_count())
        return False

    def wait_for_node_status(
        self, node_addr: str, status: str,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until a specific node has the expected status."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self.get_node_status(node_addr)
            if current == status:
                logger.info("Node %s is now %s", node_addr, status)
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for node %s to become %s", node_addr, status)
        return False

    def wait_for_shard_coverage(
        self, expected_total: int,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until total shard count matches expected."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            total = self.get_total_shard_count()
            if total == expected_total:
                logger.info("Shard coverage complete: %d", expected_total)
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for shard coverage=%d (current=%d)",
                       expected_total, self.get_total_shard_count())
        return False

    def wait_for_rebalance_complete(
        self, timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until no shard is assigned to a DEAD node."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            node_statuses = self.get_all_node_statuses()
            shard_dist = self.get_shard_distribution()
            dead_nodes = {addr for addr, s in node_statuses.items() if s == "DEAD"}
            orphaned = any(addr in dead_nodes for addr in shard_dist if shard_dist[addr] > 0)
            if not orphaned:
                logger.info("Rebalance complete, no orphaned shards")
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for rebalance to complete")
        return False

    def wait_for_stable_state(
        self, duration: float = 10, check_interval: float = 2
    ) -> bool:
        """Wait until cluster state remains unchanged for `duration` seconds."""
        last_snapshot = self.get_all_node_statuses()
        stable_since = time.time()
        while time.time() - stable_since < duration:
            time.sleep(check_interval)
            current = self.get_all_node_statuses()
            if current != last_snapshot:
                last_snapshot = current
                stable_since = time.time()
        return True

    # --- Invariant assertions ---

    def assert_no_orphaned_shards(self) -> None:
        """Assert no shard is assigned to a DEAD node."""
        node_statuses = self.get_all_node_statuses()
        shard_dist = self.get_shard_distribution()
        dead_nodes = {addr for addr, s in node_statuses.items() if s == "DEAD"}
        for addr, count in shard_dist.items():
            if addr in dead_nodes and count > 0:
                raise AssertionError(
                    f"Orphaned shards: node {addr} is DEAD but has {count} shards"
                )

    def assert_total_shard_count(self, expected: int) -> None:
        """Assert total shard count equals expected."""
        total = self.get_total_shard_count()
        assert total == expected, (
            f"Shard count mismatch: expected {expected}, got {total}"
        )

    def assert_shard_balance(self, max_imbalance_ratio: float = 0.3) -> None:
        """Assert shard distribution is roughly balanced across alive nodes."""
        node_statuses = self.get_all_node_statuses()
        shard_dist = self.get_shard_distribution()
        alive_counts = {
            addr: shard_dist.get(addr, 0)
            for addr, s in node_statuses.items()
            if s == "RUNNING"
        }
        if not alive_counts:
            return

        counts = list(alive_counts.values())
        avg = sum(counts) / len(counts)
        if avg == 0:
            return

        max_count = max(counts)
        min_count = min(counts)
        imbalance = (max_count - min_count) / avg
        assert imbalance <= max_imbalance_ratio, (
            f"Shard imbalance too high: max={max_count}, min={min_count}, "
            f"avg={avg:.1f}, imbalance={imbalance:.2f} > {max_imbalance_ratio}"
        )

    def assert_all_nodes_running(self) -> None:
        """Assert all registered nodes are RUNNING."""
        statuses = self.get_all_node_statuses()
        dead = {addr: s for addr, s in statuses.items() if s != "RUNNING"}
        assert not dead, f"Expected all nodes RUNNING, but found: {dead}"

    def get_deferred_reshard_node_count(self) -> int:
        """Count nodes in DEFERRED_RESHARD state."""
        nodes = self._list_nodes()
        return sum(1 for n in nodes if n.status == "DEFERRED_RESHARD")

    def wait_for_node_status_not(
        self, node_addr: str, status: str,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until a specific node is NOT in the given status."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            current = self.get_node_status(node_addr)
            if current is not None and current != status:
                return True
            time.sleep(poll_interval)
        return False

    # --- DS-side status verification (via UDS admin, requires PID) ---

    def get_ds_log_parser(self, node_addr: str) -> LogParser | None:
        """Get LogParser for a specific DS by its addr_str."""
        return self._ds_logs.get(node_addr)

    def get_ds_status(self, ds_host: str, ds_pid: int) -> dict[str, str]:
        """Query DS internal status via UDS admin (simmctl --pid <PID> ds status).
        Returns {"is_registered": "true/false", "cm_ready": "true/false",
                 "heartbeat_failure_count": "N"}.
        """
        try:
            return self._admin.get_ds_status(ds_host, ds_pid)
        except AdminClientError:
            return {}

    def assert_ds_is_registered(self, ds_host: str, ds_pid: int) -> None:
        """Assert DS reports itself as registered with CM."""
        status = self.get_ds_status(ds_host, ds_pid)
        assert status.get("is_registered") == "true", (
            f"DS pid={ds_pid} is_registered={status.get('is_registered')}, "
            f"expected true"
        )

    def assert_ds_cm_ready(self, ds_host: str, ds_pid: int,
                           expected: bool = True) -> None:
        """Assert DS's cm_ready flag matches expected value."""
        status = self.get_ds_status(ds_host, ds_pid)
        expected_str = "true" if expected else "false"
        assert status.get("cm_ready") == expected_str, (
            f"DS pid={ds_pid} cm_ready={status.get('cm_ready')}, "
            f"expected {expected_str}"
        )

    def assert_ds_heartbeat_failure_count(
        self, ds_host: str, ds_pid: int, min_count: int = 0,
        max_count: int | None = None
    ) -> None:
        """Assert DS heartbeat_failure_count within expected range."""
        status = self.get_ds_status(ds_host, ds_pid)
        count_str = status.get("heartbeat_failure_count", "0")
        count = int(count_str)
        if min_count > 0:
            assert count >= min_count, (
                f"DS pid={ds_pid} heartbeat_failure_count={count}, "
                f"expected >= {min_count}"
            )
        if max_count is not None:
            assert count <= max_count, (
                f"DS pid={ds_pid} heartbeat_failure_count={count}, "
                f"expected <= {max_count}"
            )

    def wait_for_ds_cm_not_ready(
        self, ds_host: str, ds_pid: int,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until DS reports cm_ready=false (detected CM failure)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.get_ds_status(ds_host, ds_pid)
            if status.get("cm_ready") == "false":
                logger.info("DS pid=%d reports cm_ready=false", ds_pid)
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for DS pid=%d cm_ready=false", ds_pid)
        return False

    def wait_for_ds_registered(
        self, ds_host: str, ds_pid: int,
        timeout: float = 60, poll_interval: float = 1.0
    ) -> bool:
        """Wait until DS reports is_registered=true and cm_ready=true."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.get_ds_status(ds_host, ds_pid)
            if (status.get("is_registered") == "true"
                    and status.get("cm_ready") == "true"):
                logger.info("DS pid=%d registered and cm_ready", ds_pid)
                return True
            time.sleep(poll_interval)
        logger.warning("Timed out waiting for DS pid=%d registration", ds_pid)
        return False

    def get_shard_distribution_for_node(self, node_addr: str) -> int:
        """Get shard count for a specific node."""
        dist = self.get_shard_distribution()
        return dist.get(node_addr, 0)

    def assert_node_has_no_shards(self, node_addr: str) -> None:
        """Assert a specific node has zero shards assigned."""
        count = self.get_shard_distribution_for_node(node_addr)
        assert count == 0, (
            f"Node {node_addr} still has {count} shards, expected 0"
        )
