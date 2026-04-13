"""Unit tests for log_parser.py — regex matching on log content.

Uses a mock SshExecutor to feed log content without actual file I/O.
"""

from unittest.mock import MagicMock

from framework.log_parser import LogParser

SAMPLE_LOG = """\
[2025-01-15 10:00:01.123] [info] ClusterManager service starts successfully!
[2025-01-15 10:00:02.456] [info] NewNodeHandshake from 10.0.0.2:40000
[2025-01-15 10:00:02.789] [info] AddNode: 10.0.0.2:40000 registered
[2025-01-15 10:00:03.100] [info] NewNodeHandshake from 10.0.0.3:40000
[2025-01-15 10:00:03.200] [info] AddNode: 10.0.0.3:40000 registered
[2025-01-15 10:00:15.000] [warn] heartbeat timeout for 10.0.0.2:40000
[2025-01-15 10:00:15.001] [warn] HandleNodeFailure: mark 10.0.0.2:40000 as DEAD
[2025-01-15 10:00:15.002] [info] RebalanceShardsAfterNodeFailure: reassign 22 shards
[2025-01-15 10:00:15.003] [info] UpdateNodeStatus: 10.0.0.2:40000 RUNNING -> DEAD
"""


def _make_parser(log_content: str) -> LogParser:
    """Create a LogParser with mocked SshExecutor."""
    ssh = MagicMock()
    ssh.read_file.return_value = log_content
    ssh.read_file_tail.return_value = (log_content, len(log_content))
    return LogParser("/fake/log.log", "127.0.0.1", ssh)


class TestLogParserPatterns:

    def test_find_handshake_events(self):
        parser = _make_parser(SAMPLE_LOG)
        events = parser.find_handshake_events()
        assert len(events) >= 4  # 2 NewNodeHandshake + 2 AddNode

    def test_find_heartbeat_timeout_events(self):
        parser = _make_parser(SAMPLE_LOG)
        events = parser.find_heartbeat_timeout_events()
        assert len(events) >= 2  # timeout + HandleNodeFailure

    def test_find_rebalance_events(self):
        parser = _make_parser(SAMPLE_LOG)
        events = parser.find_rebalance_events()
        assert len(events) >= 1

    def test_find_node_status_changes(self):
        parser = _make_parser(SAMPLE_LOG)
        events = parser.find_node_status_changes()
        assert len(events) >= 1
        assert any("DEAD" in e for e in events)

    def test_contains_existing_pattern(self):
        parser = _make_parser(SAMPLE_LOG)
        assert parser.contains("RebalanceShardsAfterNodeFailure")

    def test_contains_missing_pattern(self):
        parser = _make_parser(SAMPLE_LOG)
        assert not parser.contains("ThisPatternDoesNotExist")

    def test_count_pattern(self):
        parser = _make_parser(SAMPLE_LOG)
        count = parser.count_pattern("NewNodeHandshake")
        assert count == 2

    def test_find_pattern_lines(self):
        parser = _make_parser(SAMPLE_LOG)
        lines = parser.find_pattern_lines(r"AddNode.*registered")
        assert len(lines) == 2
        assert all("registered" in line for line in lines)


class TestLogParserEmptyLog:

    def test_empty_log(self):
        parser = _make_parser("")
        assert parser.find_handshake_events() == []
        assert parser.find_heartbeat_timeout_events() == []
        assert parser.find_rebalance_events() == []
        assert parser.count_pattern("anything") == 0
        assert not parser.contains("anything")


class TestLogParserGetAllText:

    def test_get_all_text(self):
        parser = _make_parser(SAMPLE_LOG)
        text = parser.get_all_text()
        assert "ClusterManager service starts" in text
