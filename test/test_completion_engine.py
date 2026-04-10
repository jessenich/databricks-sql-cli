import os
import pytest

from dbsqlcli.packages.completion_engine import (
    suggest_type,
    Catalog,
    Column,
    Database,
    Function,
    Alias,
    Keyword,
    Schema,
    Show,
    Table,
    View,
)


def sorted_dicts(dicts):
    """input is a list of dicts."""
    return sorted(tuple(x.items()) for x in dicts)


def test_select_suggests_cols_with_visible_table_scope():
    suggestions = suggest_type("SELECT  FROM tabl", "SELECT ")
    assert suggestions == (
        Column(tables=[(None, "tabl", None)], drop_unique=None),
        Function(schema=None, filter=None),
        Alias(aliases=["tabl"]),
        Keyword(last_token="SELECT"),
    )


def test_select_suggests_cols_with_qualified_table_scope():
    suggestions = suggest_type("SELECT  FROM sch.tabl", "SELECT ")
    assert suggestions == (
        Column(tables=[("sch", "tabl", None)], drop_unique=None),
        Function(schema=None, filter=None),
        Alias(aliases=["tabl"]),
        Keyword(last_token="SELECT"),
    )


def test_join_suggests_cols_with_qualified_table_scope():
    expression = "SELECT * FROM tabl a JOIN tabl b on a."
    suggestions = suggest_type(expression, expression)

    assert suggestions == (
        Column(tables=((None, "tabl", "a"),), drop_unique=None),
        Table(schema="a"),
        View(schema="a"),
        Function(schema="a", filter=None),
    )


def test_using_suggests_column():
    expression = "select * from a join b using("

    suggestions = suggest_type(expression, expression)

    assert suggestions == (
        Column(tables=[(None, "a", None), (None, "b", None)], drop_unique=True),
    )


@pytest.mark.parametrize(
    "expression",
    [
        "SELECT * FROM tabl WHERE ",
        "SELECT * FROM tabl WHERE (",
        "SELECT * FROM tabl WHERE foo = ",
        "SELECT * FROM tabl WHERE bar OR ",
        "SELECT * FROM tabl WHERE foo = 1 AND ",
        "SELECT * FROM tabl WHERE (bar > 10 AND ",
        "SELECT * FROM tabl WHERE (bar AND (baz OR (qux AND (",
        "SELECT * FROM tabl WHERE 10 < ",
        "SELECT * FROM tabl WHERE foo BETWEEN ",
        "SELECT * FROM tabl WHERE foo BETWEEN foo AND ",
    ],
)
def test_where_suggests_columns_functions(expression):
    suggestions = suggest_type(expression, expression)
    assert suggestions == (
        Column(tables=[(None, "tabl", None)], drop_unique=None),
        Function(schema=None, filter=None),
        Alias(aliases=["tabl"]),
        Keyword(last_token="WHERE"),
    )


def test_show_suggests_show_items_and_keywords():
    suggestions = suggest_type("SHOW ", "SHOW ")
    types = tuple(type(s) for s in suggestions)
    assert Show in types
    assert Keyword in types


def test_use_suggests_databases_and_catalogs():
    suggestions = suggest_type("USE ", "USE ")
    types = tuple(type(s) for s in suggestions)
    assert Database in types
    assert Catalog in types


def test_from_suggests_catalogs_only():
    suggestions = suggest_type("SELECT * FROM ", "SELECT * FROM ")
    types = tuple(type(s) for s in suggestions)
    assert Catalog in types
    assert Schema not in types
    assert Table not in types
    assert View not in types


def test_from_with_single_qualifier_suggests_schemas_and_tables():
    suggestions = suggest_type("SELECT * FROM myschema.", "SELECT * FROM myschema.")
    types = tuple(type(s) for s in suggestions)
    assert Catalog not in types
    assert Schema not in types
    assert Table in types
    assert View in types
    # Single qualifier could be a catalog, so Database (schemas) is suggested
    assert Database in types
    # The Database suggestion should carry the qualifier as catalog context
    db_suggestions = [s for s in suggestions if isinstance(s, Database)]
    assert db_suggestions[0].catalog == "myschema"


def test_from_with_two_level_qualifier_suggests_only_tables():
    suggestions = suggest_type(
        "SELECT * FROM catalog.schema.", "SELECT * FROM catalog.schema."
    )
    types = tuple(type(s) for s in suggestions)
    assert Table in types
    assert View in types
    # Two-level qualifier means catalog.schema is resolved — no more schema suggestions
    assert Catalog not in types
    assert Schema not in types
    assert Database not in types
    # The schema used for table lookup should be the second part
    table_suggestions = [s for s in suggestions if isinstance(s, Table)]
    assert table_suggestions[0].schema == "schema"
