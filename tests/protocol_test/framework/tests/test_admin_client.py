"""Unit tests for admin_client.py — tabulate output parsing."""

from framework.admin_client import AdminClient


# Typical simm_ctl_admin output formats
NODE_LIST_OUTPUT = """\
+-------------------+---------+
| Address           | Status  |
+-------------------+---------+
| 10.0.0.2:40000    | RUNNING |
| 10.0.0.3:40000    | RUNNING |
| 10.0.0.4:40000    | DEAD    |
+-------------------+---------+
"""

NODE_LIST_VERBOSE_OUTPUT = """\
+-------------------+---------+------------+----------+----------+
| Address           | Status  | MemTotal   | MemFree  | MemUsed  |
+-------------------+---------+------------+----------+----------+
| 10.0.0.2:40000    | RUNNING | 1024       | 512      | 512      |
| 10.0.0.3:40000    | RUNNING | 2048       | 1024     | 1024     |
+-------------------+---------+------------+----------+----------+
"""

SHARD_LIST_OUTPUT = """\
+-------------------+-------------+
| Address           | Shard Count |
+-------------------+-------------+
| 10.0.0.2:40000    | 32          |
| 10.0.0.3:40000    | 32          |
+-------------------+-------------+
"""

SHARD_LIST_VERBOSE_OUTPUT = """\
+-------------------+-------------------+
| Address           | Shard IDs         |
+-------------------+-------------------+
| 10.0.0.2:40000    | 0, 1, 2, 3       |
| 10.0.0.3:40000    | 4, 5, 6, 7       |
+-------------------+-------------------+
"""

DS_STATUS_OUTPUT = """\
+----------------------------+-------+
| Field                      | Value |
+----------------------------+-------+
| is_registered              | true  |
| cm_ready                   | true  |
| heartbeat_failure_count    | 0     |
+----------------------------+-------+
"""

FLAGS_LIST_OUTPUT = """\
+------------------------------------+---------+
| Flag Name                          | Value   |
+------------------------------------+---------+
| cm_heartbeat_timeout_inSecs        | 10      |
| heartbeat_cooldown_sec             | 2       |
+------------------------------------+---------+
"""

FLAGS_GET_OUTPUT = """\
+------------+---------+
| Key        | Result  |
+------------+---------+
| VALUE      | 10      |
+------------+---------+
"""


class TestParseTabulateRows:

    def test_node_list(self):
        rows = AdminClient._parse_tabulate_rows(NODE_LIST_OUTPUT)
        assert len(rows) == 4  # header + 3 data rows
        assert rows[0] == ["Address", "Status"]
        assert rows[1] == ["10.0.0.2:40000", "RUNNING"]
        assert rows[3] == ["10.0.0.4:40000", "DEAD"]

    def test_shard_list(self):
        rows = AdminClient._parse_tabulate_rows(SHARD_LIST_OUTPUT)
        assert len(rows) == 3  # header + 2 data
        assert rows[1] == ["10.0.0.2:40000", "32"]

    def test_shard_list_verbose(self):
        rows = AdminClient._parse_tabulate_rows(SHARD_LIST_VERBOSE_OUTPUT)
        assert len(rows) == 3
        assert rows[1] == ["10.0.0.2:40000", "0, 1, 2, 3"]

    def test_ds_status(self):
        rows = AdminClient._parse_tabulate_rows(DS_STATUS_OUTPUT)
        assert len(rows) == 4  # header + 3 fields
        assert rows[1] == ["is_registered", "true"]
        assert rows[2] == ["cm_ready", "true"]
        assert rows[3] == ["heartbeat_failure_count", "0"]

    def test_flags_list(self):
        rows = AdminClient._parse_tabulate_rows(FLAGS_LIST_OUTPUT)
        assert len(rows) == 3
        assert rows[1] == ["cm_heartbeat_timeout_inSecs", "10"]

    def test_flags_get(self):
        rows = AdminClient._parse_tabulate_rows(FLAGS_GET_OUTPUT)
        assert len(rows) == 2
        assert rows[1] == ["VALUE", "10"]

    def test_empty_output(self):
        rows = AdminClient._parse_tabulate_rows("")
        assert rows == []

    def test_separator_only(self):
        rows = AdminClient._parse_tabulate_rows("+---+---+\n+---+---+\n")
        assert rows == []

    def test_node_list_verbose(self):
        rows = AdminClient._parse_tabulate_rows(NODE_LIST_VERBOSE_OUTPUT)
        assert len(rows) == 3
        assert rows[1][0] == "10.0.0.2:40000"
        assert rows[1][2] == "1024"
        assert rows[1][4] == "512"
