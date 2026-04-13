"""Non-invasive admin client wrapping simmctl CLI tool via UDS mode.

All commands are sent through Unix domain sockets (--pid <PID>).
For remote hosts, simmctl is executed via SSH on the target node.
For local hosts, simmctl runs as a direct subprocess.
"""

import logging
from dataclasses import dataclass

from .ssh_executor import SshExecutor

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    address: str          # "ip:port"
    status: str           # "RUNNING" or "DEAD"
    mem_total_mb: int = 0
    mem_free_mb: int = 0
    mem_used_mb: int = 0


class AdminClientError(Exception):
    """Raised when admin CLI tool fails."""
    pass


class AdminClient:
    """
    Wraps simmctl as subprocess/SSH calls via UDS mode (--pid).
    Parses tabulate output to extract structured data.

    For local processes, runs simmctl as a direct subprocess.
    For remote processes, runs simmctl on the target host via SSH.
    """

    def __init__(self, ssh: SshExecutor, ctl_path: str, flags_path: str,
                 default_timeout: float = 10.0):
        """
        Args:
            ssh: SshExecutor for running commands on local/remote hosts.
            ctl_path: Path to simmctl binary on target hosts.
            flags_path: Path to simm_flags_admin binary on target hosts.
            default_timeout: Default command timeout in seconds.
        """
        self._ssh = ssh
        self._ctl_path = ctl_path
        self._flags_path = flags_path
        self._timeout = default_timeout

    def _run_ctl_uds(self, host: str, pid: int, args: list[str],
                     timeout: float | None = None) -> str:
        """Run simmctl in UDS mode (--pid) on the target host."""
        parts = [self._ctl_path, "--pid", str(pid)] + args
        cmd = " ".join(parts)
        timeout = timeout or self._timeout
        try:
            result = self._ssh.run(host, cmd, timeout=timeout, check=False)
            if result.returncode != 0:
                raise AdminClientError(
                    f"simmctl failed on {host} (rc={result.returncode}): "
                    f"{result.stderr.strip()}"
                )
            return result.stdout
        except AdminClientError:
            raise
        except Exception as e:
            raise AdminClientError(f"simmctl failed on {host}: {e}")

    def _run_flags_uds(self, host: str, pid: int, method: str,
                       flag: str = "", value: str = "",
                       timeout: float | None = None) -> str:
        """Run simm_flags_admin in UDS mode (--pid) on the target host."""
        parts = [
            self._flags_path,
            f"--pid={pid}",
            f"--method={method}",
        ]
        if flag:
            parts.append(f"--flag={flag}")
        if value:
            parts.append(f"--value={value}")

        cmd = " ".join(parts)
        timeout = timeout or self._timeout
        try:
            result = self._ssh.run(host, cmd, timeout=timeout, check=False)
            if result.returncode != 0:
                raise AdminClientError(
                    f"simm_flags_admin failed on {host} (rc={result.returncode}): "
                    f"{result.stderr.strip()}"
                )
            return result.stdout
        except AdminClientError:
            raise
        except Exception as e:
            raise AdminClientError(f"simm_flags_admin failed on {host}: {e}")

    @staticmethod
    def _parse_tabulate_rows(output: str) -> list[list[str]]:
        """
        Parse tabulate table output into rows of cell values.
        Tabulate format uses | as column separator and +---+ as row separator.
        """
        rows = []
        for line in output.splitlines():
            line = line.strip()
            if not line or line.startswith("+"):
                continue
            if "|" in line:
                cells = [cell.strip() for cell in line.split("|")]
                # Remove empty first/last elements from leading/trailing |
                cells = [c for c in cells if c]
                if cells:
                    rows.append(cells)
        return rows

    # --- Node operations (via CM admin UDS) ---

    def list_nodes(self, host: str, cm_pid: int,
                   verbose: bool = False) -> list[NodeInfo]:
        """List all nodes via simmctl --pid <CM_PID> node list."""
        args = ["node", "list"]
        if verbose:
            args.append("--verbose")
        output = self._run_ctl_uds(host, cm_pid, args)
        rows = self._parse_tabulate_rows(output)

        nodes = []
        for row in rows[1:]:  # skip header row
            if verbose and len(row) >= 5:
                nodes.append(NodeInfo(
                    address=row[0],
                    status=row[1],
                    mem_total_mb=int(row[2]) if row[2].isdigit() else 0,
                    mem_free_mb=int(row[3]) if row[3].isdigit() else 0,
                    mem_used_mb=int(row[4]) if row[4].isdigit() else 0,
                ))
            elif len(row) >= 2:
                nodes.append(NodeInfo(address=row[0], status=row[1]))
        return nodes

    # --- Shard operations (via CM admin UDS) ---

    def list_shards(self, host: str, cm_pid: int) -> dict[str, int]:
        """
        List shard distribution via simmctl --pid <CM_PID> shard list.
        Returns {node_addr: shard_count}.
        """
        output = self._run_ctl_uds(host, cm_pid, ["shard", "list"])
        rows = self._parse_tabulate_rows(output)

        distribution: dict[str, int] = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2:
                distribution[row[0]] = int(row[1])
        return distribution

    def list_shards_verbose(self, host: str, cm_pid: int) -> dict[str, list[int]]:
        """
        List detailed shard assignment via simmctl --pid <CM_PID> shard list --verbose.
        Returns {node_addr: [shard_ids]}.
        """
        output = self._run_ctl_uds(host, cm_pid,
                                   ["shard", "list", "--verbose"])
        rows = self._parse_tabulate_rows(output)

        distribution: dict[str, list[int]] = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2:
                shard_ids = [int(s.strip()) for s in row[1].split(",") if s.strip().isdigit()]
                distribution[row[0]] = shard_ids
        return distribution

    # --- DS status operations (via DS admin UDS) ---

    def get_ds_status(self, host: str, ds_pid: int) -> dict[str, str]:
        """
        Query DS internal status via simmctl --pid <PID> ds status.
        Runs on the DS host to access /run/simm/admin_ds.<pid>.sock.
        Returns {"is_registered": "true"/"false",
                 "cm_ready": "true"/"false",
                 "heartbeat_failure_count": "N"}.
        """
        output = self._run_ctl_uds(host, ds_pid, ["ds", "status"])
        rows = self._parse_tabulate_rows(output)
        status = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2:
                status[row[0]] = row[1]
        return status

    # --- GFlag operations (via UDS) ---

    def get_flag(self, host: str, pid: int, flag_name: str) -> str | None:
        """Get a single flag value via UDS."""
        try:
            output = self._run_ctl_uds(host, pid, ["gflag", "get", flag_name])
            rows = self._parse_tabulate_rows(output)
            for row in rows:
                if len(row) >= 2 and row[0] == "VALUE":
                    return row[1]
            return None
        except AdminClientError as e:
            logger.error("get_flag failed: %s", e)
            return None

    def set_flag(self, host: str, pid: int,
                 flag_name: str, value: str) -> bool:
        """Set a flag value via UDS."""
        try:
            self._run_ctl_uds(host, pid, ["gflag", "set", flag_name, value])
            return True
        except AdminClientError as e:
            logger.error("set_flag failed: %s", e)
            return False

    def list_flags(self, host: str, pid: int) -> dict[str, str]:
        """List all flags via UDS. Returns {flag_name: value}."""
        try:
            output = self._run_ctl_uds(host, pid, ["gflag", "list"])
            rows = self._parse_tabulate_rows(output)
            flags = {}
            for row in rows[1:]:  # skip header
                if len(row) >= 2:
                    flags[row[0]] = row[1]
            return flags
        except AdminClientError as e:
            logger.error("list_flags failed: %s", e)
            return {}
