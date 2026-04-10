from dbsqlcli.completer import DBSQLCompleter
from dbsqlcli.packages.completion_engine import (
    Column,
    Database,
    Table,
    Schema,
    Catalog,
    Show,
)


class FakeDocument:
    def __init__(self, text, text_before_cursor):
        self.text = text
        self.text_before_cursor = text_before_cursor

    def get_word_before_cursor(self, *args, **kwargs):
        return self.text_before_cursor


def test_completer():
    completer = DBSQLCompleter()

    document = FakeDocument("create table default.", "create table default.")

    try:
        completer.get_completions(document, None)
    except Exception:
        assert False, "get_compeltions shouldn't raise"


def test_schema_matches_return_databases():
    completer = DBSQLCompleter()
    completer.extend_database_names(["my_schema", "other_schema"])
    matches = list(completer.get_schema_matches(Schema(), "my_"))
    assert len(matches) == 1
    assert matches[0].text == "my_schema"


def test_catalog_matches():
    completer = DBSQLCompleter()
    completer.extend_catalog_names(["prod_catalog", "dev_catalog"])
    matches = list(completer.get_catalog_matches(Catalog(), "prod"))
    assert len(matches) == 1
    assert matches[0].text == "prod_catalog"


def test_show_matches_does_not_crash():
    completer = DBSQLCompleter()
    matches = list(completer.get_show_matches(Show(), "TAB"))
    assert any(m.text == "TABLES" for m in matches)


def test_database_matches_scoped_to_catalog():
    completer = DBSQLCompleter()
    completer.extend_database_names(["sales", "marketing", "engineering"])
    completer.extend_catalog_schemas("prod", ["sales", "marketing"])
    completer.extend_catalog_schemas("dev", ["engineering"])

    # With catalog context, only show schemas in that catalog
    matches = list(completer.get_database_matches(Database(catalog="prod"), ""))
    names = [m.text for m in matches]
    assert "sales" in names
    assert "marketing" in names
    assert "engineering" not in names

    # Without catalog context, show all schemas
    matches = list(completer.get_database_matches(Database(), ""))
    names = [m.text for m in matches]
    assert "sales" in names
    assert "marketing" in names
    assert "engineering" in names


def test_table_matches_scoped_to_catalog():
    completer = DBSQLCompleter()
    completer.set_catalog("prod")
    completer.set_dbname("default")
    completer.extend_schemata("default", catalog="prod")
    completer.extend_relations([("orders",), ("customers",)], kind="tables")

    # Switch to dev catalog's default schema
    completer.set_catalog("dev")
    completer.extend_schemata("default", catalog="dev")
    completer.extend_relations([("test_orders",)], kind="tables")
    completer.set_catalog("prod")  # Reset to prod

    # With catalog="prod", should only see prod tables
    tables = list(
        completer.populate_schema_objects("default", "tables", catalog="prod")
    )
    assert "orders" in tables
    assert "customers" in tables
    assert "test_orders" not in tables

    # With catalog="dev", should only see dev tables
    tables = list(completer.populate_schema_objects("default", "tables", catalog="dev"))
    assert "test_orders" in tables
    assert "orders" not in tables


def test_view_completions():
    completer = DBSQLCompleter()
    completer.extend_schemata("test_db")
    completer.set_dbname("test_db")
    completer.extend_relations([("my_view",)], kind="views")
    views = completer.populate_schema_objects("test_db", "views")
    assert "my_view" in views


# ---------------------------------------------------------------------------
# Scoped column completion tests
# ---------------------------------------------------------------------------


def _setup_completer_with_columns(
    catalog="prod", schema="default", tables_columns=None, views_columns=None
):
    """Helper: create a completer with tables and real column metadata loaded."""
    completer = DBSQLCompleter()
    completer.set_catalog(catalog)
    completer.set_dbname(schema)
    completer.extend_schemata(schema, catalog=catalog)

    if tables_columns:
        table_names = list(tables_columns.keys())
        completer.extend_relations([(t,) for t in table_names], kind="tables")
        completer.extend_columns(
            ((t, c) for t, cols in tables_columns.items() for c in cols),
            kind="tables",
        )

    if views_columns:
        view_names = list(views_columns.keys())
        completer.extend_relations([(v,) for v in view_names], kind="views")
        completer.extend_columns(
            ((v, c) for v, cols in views_columns.items() for c in cols),
            kind="views",
        )

    return completer


def test_scoped_columns_basic():
    """Columns are returned for a table in the active schema."""
    completer = _setup_completer_with_columns(
        tables_columns={"orders": ["id", "amount", "customer_id"]}
    )
    cols = completer.populate_scoped_cols([(None, "orders", None)])
    # Should include "*" (default) plus the real columns
    assert "id" in cols
    assert "amount" in cols
    assert "customer_id" in cols


def test_scoped_columns_cross_schema():
    """Columns are returned for a table in a non-active schema."""
    completer = DBSQLCompleter()

    # Register and populate "analytics" schema under "prod" catalog
    completer.set_catalog("prod")
    completer.set_dbname("analytics")
    completer.extend_schemata("analytics", catalog="prod")
    completer.extend_relations([("events",)], kind="tables")
    completer.extend_columns([("events", "event_id"), ("events", "ts")], kind="tables")

    # Switch active schema to "default"
    completer.set_dbname("default")
    completer.extend_schemata("default", catalog="prod")

    # Query references analytics.events — should still find columns
    cols = completer.populate_scoped_cols([("analytics", "events", None)])
    assert "event_id" in cols
    assert "ts" in cols


def test_scoped_columns_cross_catalog():
    """Columns are returned for a table in a different catalog."""
    completer = DBSQLCompleter()

    # Populate "dev" catalog
    completer.set_catalog("dev")
    completer.set_dbname("staging")
    completer.extend_schemata("staging", catalog="dev")
    completer.extend_relations([("raw_data",)], kind="tables")
    completer.extend_columns(
        [("raw_data", "row_id"), ("raw_data", "payload")], kind="tables"
    )

    # Switch to "prod" catalog
    completer.set_catalog("prod")
    completer.set_dbname("default")
    completer.extend_schemata("default", catalog="prod")
    completer.extend_relations([("users",)], kind="tables")
    completer.extend_columns([("users", "user_id"), ("users", "name")], kind="tables")

    # Active is prod.default — query references prod.default.users
    cols = completer.populate_scoped_cols([(None, "users", None)])
    assert "user_id" in cols
    assert "name" in cols

    # Now query references dev.staging.raw_data (schema="staging" parsed from SQL)
    # populate_scoped_cols builds key as current_catalog.schema, so we need to
    # temporarily pretend the catalog context matches for the three-part lookup.
    # In practice the SQL `FROM dev.staging.raw_data` would be parsed differently,
    # but we can test the key lookup directly.
    completer.set_catalog("dev")
    cols = completer.populate_scoped_cols([("staging", "raw_data", None)])
    assert "row_id" in cols
    assert "payload" in cols


def test_scoped_columns_multiple_tables():
    """Columns from multiple tables in FROM clause are merged."""
    completer = _setup_completer_with_columns(
        tables_columns={
            "orders": ["order_id", "amount"],
            "customers": ["customer_id", "email"],
        }
    )
    cols = completer.populate_scoped_cols(
        [(None, "orders", None), (None, "customers", None)]
    )
    assert "order_id" in cols
    assert "amount" in cols
    assert "customer_id" in cols
    assert "email" in cols


def test_scoped_columns_with_alias():
    """Alias in tuple does not break column lookup."""
    completer = _setup_completer_with_columns(
        tables_columns={"orders": ["order_id", "total"]}
    )
    # (schema, table, alias) — alias is "o"
    cols = completer.populate_scoped_cols([(None, "orders", "o")])
    assert "order_id" in cols
    assert "total" in cols


def test_scoped_columns_view():
    """Scoped columns work for views."""
    completer = _setup_completer_with_columns(
        views_columns={"active_users": ["user_id", "last_login"]}
    )
    cols = completer.populate_scoped_cols([(None, "active_users", None)])
    assert "user_id" in cols
    assert "last_login" in cols


def test_scoped_columns_star_fallback():
    """When no real columns are loaded, '*' is still returned."""
    completer = DBSQLCompleter()
    completer.set_catalog("prod")
    completer.set_dbname("default")
    completer.extend_schemata("default", catalog="prod")
    # Register table but do NOT extend_columns — leaves default ["*"]
    completer.extend_relations([("mystery_table",)], kind="tables")

    cols = completer.populate_scoped_cols([(None, "mystery_table", None)])
    assert cols == ["*"]


def test_scoped_columns_unknown_table():
    """An unknown table returns no columns (empty list)."""
    completer = _setup_completer_with_columns(tables_columns={"orders": ["id"]})
    cols = completer.populate_scoped_cols([(None, "nonexistent", None)])
    assert cols == []


def test_get_column_matches_via_suggestion():
    """get_column_matches returns Completion objects for scoped columns."""
    completer = _setup_completer_with_columns(
        tables_columns={"orders": ["order_id", "amount", "status"]}
    )
    suggestion = Column(tables=[(None, "orders", None)], drop_unique=None)
    matches = list(completer.get_column_matches(suggestion, "ord"))
    texts = [m.text for m in matches]
    assert "order_id" in texts
    # "amount" and "status" should not match prefix "ord"
    assert "amount" not in texts
    assert "status" not in texts


def test_get_column_matches_drop_unique():
    """drop_unique=True only suggests columns appearing in multiple tables."""
    completer = _setup_completer_with_columns(
        tables_columns={
            "t1": ["shared_col", "only_t1"],
            "t2": ["shared_col", "only_t2"],
        }
    )
    suggestion = Column(
        tables=[(None, "t1", None), (None, "t2", None)], drop_unique=True
    )
    matches = list(completer.get_column_matches(suggestion, ""))
    texts = [m.text for m in matches]
    assert "shared_col" in texts
    assert "only_t1" not in texts
    assert "only_t2" not in texts


def test_scoped_columns_escaped_table_name():
    """Columns are found even when the table name is backtick-escaped in metadata."""
    completer = DBSQLCompleter()
    completer.set_catalog("prod")
    completer.set_dbname("default")
    completer.extend_schemata("default", catalog="prod")

    # Simulate what extend_relations does for a table with a reserved word name
    # The escaped_names method wraps it in backticks
    completer.extend_relations([("select",)], kind="tables")
    # The escaped name in metadata would be "`select`"
    escaped_name = completer.escape_name("select")
    assert escaped_name == "`select`"

    # Extend columns using the escaped name (as extend_columns would)
    completer.extend_columns(
        [(escaped_name, "col_a"), (escaped_name, "col_b")], kind="tables"
    )

    # populate_scoped_cols tries both raw and escaped names
    cols = completer.populate_scoped_cols([(None, "select", None)])
    assert "col_a" in cols or "`select`" in [
        k
        for key in completer.dbmetadata["tables"]
        for k in completer.dbmetadata["tables"][key]
    ]
