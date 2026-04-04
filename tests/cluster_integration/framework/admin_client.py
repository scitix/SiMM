"""Non-invasive admin client wrapping simm_ctl_admin and simm_flags_admin CLI tools.

Admin CLI tools are always run locally on the test runner node. They communicate
with remote CM/DS nodes via SiCL RPC (--ip/--port point to the remote node).
No SSH is needed for admin operations.
"""

import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

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
    Wraps simm_ctl_admin and simm_flags_admin as subprocess calls.
    Parses their tabulate output to extract structured data.

    These CLI tools run locally on the test runner and connect to
    remote CM/DS nodes via RPC using the --ip/--port parameters.
    """

    def __init__(self, ctl_binary: Path, flags_binary: Path,
                 default_timeout: float = 10.0):
        self._ctl = Path(ctl_binary)
        self._flags = Path(flags_binary)
        self._timeout = default_timeout

    def _run_ctl(self, ip: str, port: int, args: list[str],
                 timeout: float | None = None) -> str:
        """Run simm_ctl_admin and return stdout."""
        cmd = [str(self._ctl), "--ip", ip, "--port", str(port)] + args
        timeout = timeout or self._timeout
        logger.debug("Running: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                raise AdminClientError(
                    f"simm_ctl_admin failed (rc={result.returncode}): {result.stderr}"
                )
            return result.stdout
        except subprocess.TimeoutExpired:
            raise AdminClientError(f"simm_ctl_admin timed out after {timeout}s")
        except FileNotFoundError:
            raise AdminClientError(f"simm_ctl_admin not found at {self._ctl}")

    def _run_flags(self, ip: str, port: int, method: str,
                   flag: str = "", value: str = "",
                   timeout: float | None = None) -> str:
        """Run simm_flags_admin and return stdout."""
        cmd = [
            str(self._flags),
            f"--ip={ip}",
            f"--port={port}",
            f"--method={method}",
        ]
        if flag:
            cmd.append(f"--flag={flag}")
        if value:
            cmd.append(f"--value={value}")

        timeout = timeout or self._timeout
        logger.debug("Running: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                raise AdminClientError(
                    f"simm_flags_admin failed (rc={result.returncode}): {result.stderr}"
                )
            return result.stdout
        except subprocess.TimeoutExpired:
            raise AdminClientError(f"simm_flags_admin timed out after {timeout}s")
        except FileNotFoundError:
            raise AdminClientError(f"simm_flags_admin not found at {self._flags}")

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

    # --- Node operations ---

    def list_nodes(self, cm_ip: str, cm_admin_port: int,
                   verbose: bool = False) -> list[NodeInfo]:
        """List all nodes via simm_ctl_admin node list."""
        args = ["node", "list"]
        if verbose:
            args.append("--verbose")
        output = self._run_ctl(cm_ip, cm_admin_port, args)
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

    def set_node_status(self, cm_ip: str, cm_admin_port: int,
                        node_addr: str, status: str) -> bool:
        """Set node status via simm_ctl_admin node set."""
        try:
            self._run_ctl(cm_ip, cm_admin_port,
                          ["node", "set", node_addr, status])
            return True
        except AdminClientError as e:
            logger.error("set_node_status failed: %s", e)
            return False

    # --- Shard operations ---

    def list_shards(self, cm_ip: str, cm_admin_port: int) -> dict[str, int]:
        """
        List shard distribution via simm_ctl_admin shard list.
        Returns {node_addr: shard_count}.
        """
        output = self._run_ctl(cm_ip, cm_admin_port, ["shard", "list"])
        rows = self._parse_tabulate_rows(output)

        distribution: dict[str, int] = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2:
                distribution[row[0]] = int(row[1])
        return distribution

    def list_shards_verbose(self, cm_ip: str, cm_admin_port: int) -> dict[str, list[int]]:
        """
        List detailed shard assignment via simm_ctl_admin shard list --verbose.
        Returns {node_addr: [shard_ids]}.
        """
        output = self._run_ctl(cm_ip, cm_admin_port,
                               ["shard", "list", "--verbose"])
        rows = self._parse_tabulate_rows(output)

        distribution: dict[str, list[int]] = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2:
                shard_ids = [int(s.strip()) for s in row[1].split(",") if s.strip().isdigit()]
                distribution[row[0]] = shard_ids
        return distribution

    # --- DS status operations (via UDS, requires PID) ---

    def get_ds_status(self, ds_admin_name: str, ds_pid: int) -> dict[str, str]:
        """
        Query DS internal status via simm_ctl_admin --name <NAME> --pid <PID> ds status.
        Uses Unix domain socket /run/simm/simm_<name>.<pid>.sock on the DS host.
        Returns {"is_registered": "true"/"false",
                 "cm_ready": "true"/"false",
                 "heartbeat_failure_count": "N"}.
        """
        cmd = [
            str(self._ctl),
            "--name", ds_admin_name,
            "--pid", str(ds_pid),
            "ds", "status",
        ]
        timeout = self._timeout
        logger.debug("Running: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                raise AdminClientError(
                    f"simm_ctl_admin ds status failed (rc={result.returncode}): "
                    f"{result.stderr}"
                )
            rows = self._parse_tabulate_rows(result.stdout)
            status = {}
            for row in rows[1:]:  # skip header
                if len(row) >= 2:
                    status[row[0]] = row[1]
            return status
        except subprocess.TimeoutExpired:
            raise AdminClientError(
                f"simm_ctl_admin ds status timed out after {timeout}s"
            )
        except FileNotFoundError:
            raise AdminClientError(
                f"simm_ctl_admin not found at {self._ctl}"
            )

    # --- GFlag operations ---

    def get_flag(self, ip: str, port: int, flag_name: str) -> str | None:
        """Get a single flag value."""
        try:
            output = self._run_flags(ip, port, "get", flag=flag_name)
            rows = self._parse_tabulate_rows(output)
            # Output format: two-column table with "Flag Name" / "VALUE" rows
            for row in rows:
                if len(row) >= 2 and row[0] == "VALUE":
                    return row[1]
            return None
        except AdminClientError as e:
            logger.error("get_flag failed: %s", e)
            return None

    def set_flag(self, ip: str, port: int,
                 flag_name: str, value: str) -> bool:
        """Set a flag value."""
        try:
            self._run_flags(ip, port, "set", flag=flag_name, value=value)
            return True
        except AdminClientError as e:
            logger.error("set_flag failed: %s", e)
            return False

    def list_flags(self, ip: str, port: int) -> dict[str, str]:
        """List all flags. Returns {flag_name: value}."""
        try:
            output = self._run_flags(ip, port, "list")
            rows = self._parse_tabulate_rows(output)
            flags = {}
            for row in rows[1:]:  # skip header
                if len(row) >= 2:
                    flags[row[0]] = row[1]
            return flags
        except AdminClientError as e:
            logger.error("list_flags failed: %s", e)
            return {}
