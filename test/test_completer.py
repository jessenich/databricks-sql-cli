from dbsqlcli.completer import DBSQLCompleter
from dbsqlcli.packages.completion_engine import Database, Table, Schema, Catalog, Show


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
