# -*- coding: utf-8 -*-


from collections import namedtuple
from datetime import datetime, date, time
from dbsqlcli.packages.format_utils import format_status, humanize_size
from dbsqlcli.main import _json_safe_preprocessor


def test_format_status_plural():
    assert format_status(rows_length=1) == "1 row in set"
    assert format_status(rows_length=2) == "2 rows in set"


def test_format_status_no_results():
    assert format_status(rows_length=None) == "Query OK"


def test_humanize_size():
    assert humanize_size(20) == "20 B"
    assert humanize_size(2000) == "1.95 KB"
    assert humanize_size(200000) == "195.31 KB"
    assert humanize_size(20000000) == "19.07 MB"
    assert humanize_size(200000000000) == "186.26 GB"


def test_json_safe_preprocessor_converts_datetime():
    headers = ["id", "created_at", "event_date", "event_time"]
    data = [
        (1, datetime(2025, 1, 15, 10, 30, 0), date(2025, 1, 15), time(10, 30, 0)),
        (2, datetime(2025, 6, 1, 0, 0, 0), date(2025, 6, 1), time(0, 0, 0)),
    ]
    result_data, result_headers = _json_safe_preprocessor(data, headers)
    assert result_headers == headers
    assert result_data == [
        (1, "2025-01-15T10:30:00", "2025-01-15", "10:30:00"),
        (2, "2025-06-01T00:00:00", "2025-06-01", "00:00:00"),
    ]


def test_json_safe_preprocessor_preserves_non_datetime():
    headers = ["id", "name", "score"]
    data = [(1, "alice", 95.5), (2, None, 87)]
    result_data, result_headers = _json_safe_preprocessor(data, headers)
    assert result_headers == headers
    assert result_data == [(1, "alice", 95.5), (2, None, 87)]
