"""Parse CM/DS log files for state verification — supports local and remote hosts.

In multi-machine mode, log files reside on remote hosts. LogParser reads them
via SshExecutor instead of direct file access.
"""

import re
import time
from dataclasses import dataclass

from .ssh_executor import SshExecutor


@dataclass
class LogEvent:
    timestamp: str
    level: str
    message: str
    raw_line: str


class LogParser:
    """Parses SiMM log files for state verification events.

    Supports reading logs from both local and remote hosts via SshExecutor.
    """

    # Common log patterns in SiMM (based on spdlog format: [timestamp] [level] message)
    LOG_LINE_RE = re.compile(
        r'\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)\]\s+'
        r'\[(\w+)\]\s+'
        r'(.*)'
    )

    def __init__(self, log_path: str, host: str, ssh_executor: SshExecutor):
        """
        Args:
            log_path: Path to the log file on the target host.
            host: Hostname/IP where the log file resides.
            ssh_executor: SSH executor for reading remote files.
        """
        self._log_path = log_path
        self._host = host
        self._ssh = ssh_executor
        self._last_offset = 0

    def _read_all_lines(self) -> list[str]:
        content = self._ssh.read_file(self._host, self._log_path)
        if not content:
            return []
        return content.splitlines()

    def _read_new_lines(self) -> list[str]:
        """Read only new lines since last read."""
        content, new_offset = self._ssh.read_file_tail(
            self._host, self._log_path, self._last_offset
        )
        self._last_offset = new_offset
        if not content:
            return []
        return [line.rstrip() for line in content.splitlines()]

    def wait_for_pattern(self, pattern: str, timeout: float = 30,
                         poll_interval: float = 0.5) -> str | None:
        """Tail log file waiting for a regex pattern match. Returns the matching line."""
        compiled = re.compile(pattern)
        deadline = time.time() + timeout
        while time.time() < deadline:
            lines = self._read_new_lines()
            for line in lines:
                if compiled.search(line):
                    return line
            time.sleep(poll_interval)
        return None

    def count_pattern(self, pattern: str) -> int:
        """Count occurrences of a pattern in the entire log."""
        compiled = re.compile(pattern)
        return sum(1 for line in self._read_all_lines() if compiled.search(line))

    def find_pattern_lines(self, pattern: str) -> list[str]:
        """Return all lines matching a pattern."""
        compiled = re.compile(pattern)
        return [line for line in self._read_all_lines() if compiled.search(line)]

    def find_handshake_events(self) -> list[str]:
        """Find node handshake/registration log entries."""
        patterns = [
            r"[Hh]andshake",
            r"AddNode",
            r"NewNodeHandshake",
            r"[Rr]egister.*[Cc]luster",
        ]
        combined = "|".join(f"({p})" for p in patterns)
        return self.find_pattern_lines(combined)

    def find_heartbeat_timeout_events(self) -> list[str]:
        """Find heartbeat timeout / node failure detection log entries."""
        patterns = [
            r"heartbeat.*timeout",
            r"[Nn]ode.*[Ff]ailure",
            r"mark.*[Dd]ead",
            r"DEAD",
            r"HandleNodeFailure",
        ]
        combined = "|".join(f"({p})" for p in patterns)
        return self.find_pattern_lines(combined)

    def find_rebalance_events(self) -> list[str]:
        """Find shard rebalance log entries."""
        patterns = [
            r"[Rr]ebalance",
            r"[Rr]eassign.*[Ss]hard",
            r"[Ss]hard.*[Mm]igrat",
            r"RebalanceShardsAfterNodeFailure",
        ]
        combined = "|".join(f"({p})" for p in patterns)
        return self.find_pattern_lines(combined)

    def find_node_status_changes(self) -> list[str]:
        """Find node status transition log entries."""
        patterns = [
            r"UpdateNodeStatus",
            r"RUNNING.*DEAD|DEAD.*RUNNING",
            r"node.*status.*changed",
        ]
        combined = "|".join(f"({p})" for p in patterns)
        return self.find_pattern_lines(combined)

    def contains(self, pattern: str) -> bool:
        """Check if log contains at least one match of pattern."""
        return self.count_pattern(pattern) > 0

    def get_all_text(self) -> str:
        """Return full log text."""
        return self._ssh.read_file(self._host, self._log_path)
