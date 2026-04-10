import time
import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def refresher():
    from dbsqlcli.completion_refresher import CompletionRefresher

    return CompletionRefresher()


def test_ctor(refresher):
    """Refresher object should contain a few handlers.

    :param refresher:
    :return:

    """
    assert len(refresher.refreshers) > 0
    actual_handlers = list(refresher.refreshers.keys())
    expected_handlers = [
        "catalogs",
        "databases",
        "schemata",
        "tables",
        "special_commands",
    ]
    assert expected_handlers == actual_handlers


def test_refresh_called_once(refresher):
    """

    :param refresher:
    :return:
    """
    callbacks = Mock()
    sqlexecute = Mock()

    with patch.object(refresher, "_bg_refresh") as bg_refresh:
        actual = refresher.refresh(sqlexecute, callbacks)
        time.sleep(1)  # Wait for the thread to work.
        assert len(actual) == 1
        assert len(actual[0]) == 4
        assert actual[0][3] == "Auto-completion refresh started in the background."
        bg_refresh.assert_called_with(sqlexecute, callbacks, {})


def test_refresh_called_twice(refresher):
    """If refresh is called a second time, it should be restarted.

    :param refresher:
    :return:

    """
    callbacks = Mock()

    sqlexecute = Mock()

    def dummy_bg_refresh(*args):
        time.sleep(3)  # seconds

    refresher._bg_refresh = dummy_bg_refresh

    actual1 = refresher.refresh(sqlexecute, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert len(actual1) == 1
    assert len(actual1[0]) == 4
    assert actual1[0][3] == "Auto-completion refresh started in the background."

    actual2 = refresher.refresh(sqlexecute, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert len(actual2) == 1
    assert len(actual2[0]) == 4
    assert actual2[0][3] == "Auto-completion refresh restarted."


def test_bulk_column_insertion():
    """Verify columns loaded via refresh_tables bulk path are retrievable
    by populate_scoped_cols (end-to-end through the actual refresher code)."""
    from unittest.mock import Mock
    from dbsqlcli.completer import DBSQLCompleter
    from dbsqlcli.completion_refresher import (
        refresh_catalogs,
        refresh_databases,
        refresh_schemata,
        refresh_tables,
    )

    executor = Mock()
    executor.current_catalog.return_value = "main"
    executor.databases.return_value = ["default", "analytics"]
    executor.catalog_schema_map.return_value = {
        "main": ["default", "analytics"],
    }
    executor.catalog_table_map.return_value = {
        ("main", "default"): [
            ("orders", "TABLE"),
            ("customers", "TABLE"),
            ("active_users", "VIEW"),
        ],
        ("main", "analytics"): [
            ("events", "TABLE"),
        ],
    }
    executor.catalog_column_map.return_value = {
        ("main", "default"): [
            ("orders", "order_id"),
            ("orders", "amount"),
            ("customers", "customer_id"),
            ("customers", "email"),
            ("active_users", "user_id"),
            ("active_users", "last_login"),
        ],
        ("main", "analytics"): [
            ("events", "event_id"),
            ("events", "timestamp"),
        ],
    }
    executor.database = "default"

    completer = DBSQLCompleter()

    # Run refreshers in order, just like _bg_refresh does
    refresh_catalogs(completer, executor)
    refresh_databases(completer, executor)
    refresh_schemata(completer, executor)
    refresh_tables(completer, executor)

    # Active schema is main.default — columns should be found for tables there
    cols = completer.populate_scoped_cols([(None, "orders", None)])
    assert "order_id" in cols, f"Expected 'order_id' in {cols}"
    assert "amount" in cols, f"Expected 'amount' in {cols}"

    cols = completer.populate_scoped_cols([(None, "customers", None)])
    assert "customer_id" in cols
    assert "email" in cols

    # View columns
    cols = completer.populate_scoped_cols([(None, "active_users", None)])
    assert "user_id" in cols
    assert "last_login" in cols

    # Cross-schema: analytics.events (need to qualify with schema)
    cols = completer.populate_scoped_cols([("analytics", "events", None)])
    assert "event_id" in cols, f"Expected 'event_id' in {cols}"
    assert "timestamp" in cols

    # Columns should also be in all_completions for non-smart mode
    assert "order_id" in completer.all_completions
    assert "event_id" in completer.all_completions


def test_bulk_columns_appear_in_completions():
    """Full end-to-end: bulk-loaded columns appear as completions for
    'SELECT  FROM orders' when using smart completion."""
    from unittest.mock import Mock
    from dbsqlcli.completer import DBSQLCompleter
    from dbsqlcli.completion_refresher import (
        refresh_catalogs,
        refresh_databases,
        refresh_schemata,
        refresh_tables,
    )

    executor = Mock()
    executor.current_catalog.return_value = "main"
    executor.databases.return_value = ["default"]
    executor.catalog_schema_map.return_value = {"main": ["default"]}
    executor.catalog_table_map.return_value = {
        ("main", "default"): [("orders", "TABLE")],
    }
    executor.catalog_column_map.return_value = {
        ("main", "default"): [
            ("orders", "order_id"),
            ("orders", "amount"),
            ("orders", "status"),
        ],
    }
    executor.database = "default"

    completer = DBSQLCompleter()
    refresh_catalogs(completer, executor)
    refresh_databases(completer, executor)
    refresh_schemata(completer, executor)
    refresh_tables(completer, executor)

    # Simulate typing "SELECT  FROM orders" with cursor after "SELECT "
    class FakeDoc:
        def __init__(self, text, before):
            self.text = text
            self.text_before_cursor = before
        def get_word_before_cursor(self, **kw):
            return ""

    doc = FakeDoc("SELECT  FROM orders", "SELECT ")
    completions = list(completer.get_completions(doc, None))
    texts = [c.text for c in completions]

    assert "order_id" in texts, f"Expected 'order_id' in completions, got {texts}"
    assert "amount" in texts, f"Expected 'amount' in completions, got {texts}"


def test_refresh_with_callbacks(refresher):
    """Callbacks must be called.

    :param refresher:

    """
    callbacks = [Mock()]
    sqlexecute_class = Mock()
    sqlexecute = Mock()

    with patch("dbsqlcli.completion_refresher.SQLExecute", sqlexecute_class):
        # Set refreshers to 0: we're not testing refresh logic here
        refresher.refreshers = {}
        refresher.refresh(sqlexecute, callbacks)
        time.sleep(1)  # Wait for the thread to work.
        assert callbacks[0].call_count == 1
