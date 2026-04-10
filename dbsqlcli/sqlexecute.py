# encoding: utf-8
from typing import Optional
import logging
import sqlparse, click
from databricks import sql as dbsql

from dbsqlcli.packages import special
from dbsqlcli.packages.format_utils import format_status
from databricks.sql.exc import RequestError

from databricks.sql.experimental.oauth_persistence import OAuthPersistence, OAuthToken
from databricks.sql.auth.auth import AuthType

logger = logging.getLogger(__name__)

from dbsqlcli import __version__ as CURRENT_VERSION

USER_AGENT_STRING = f"DBSQLCLI/{CURRENT_VERSION}"

DBSQL_CLI_OAUTH_CLIENT_ID = "databricks-cli"
DBSQL_CLI_OAUTH_PORT = 8020


class OAuthPersistenceCache(OAuthPersistence):
    def __init__(self):
        self.tokens = {}

    def persist(self, hostname: str, oauth_token: OAuthToken):
        self.tokens[hostname] = oauth_token

    def read(self, hostname: str) -> Optional[OAuthToken]:
        return self.tokens.get(hostname)


oauth_token_cache = OAuthPersistenceCache()


class SQLExecute(object):
    DATABASES_QUERY = "SHOW DATABASES"

    def __init__(self, hostname, http_path, access_token, database, auth_type=None):
        self.hostname = hostname
        self.http_path = http_path
        self.access_token = access_token
        self.database = database or "default"
        self.auth_type = auth_type

        self.connect(database=self.database)

    def connect(self, database=None):
        self.close_connection()

        oauth_params = {}
        if self.auth_type == AuthType.DATABRICKS_OAUTH.value:
            oauth_params = {
                "auth_type": self.auth_type,
                "experimental_oauth_persistence": oauth_token_cache,
                "oauth_client_id": DBSQL_CLI_OAUTH_CLIENT_ID,
                "oauth_redirect_port": DBSQL_CLI_OAUTH_PORT,
            }

        conn = dbsql.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            schema=database,
            user_agent_entry=USER_AGENT_STRING,
            **oauth_params,
        )

        self.database = database or self.database

        self.conn = conn

    def reconnect(self):

        self.close_connection()
        self.connect(database=self.database)

    def close_connection(self):
        """Close any open connection and remove the `conn` attribute"""

        if not hasattr(self, "conn"):
            return

        try:
            self.conn.close()
        except AttributeError as e:
            logger.debug("There is no active connection to close.")
            delattr(self, "conn")
        except RequestError as e:
            message = "The connection is no longer active and will be recycled. It was probably was timed-out by SQL gateway"
            click.echo(message)
            logger.debug(f"{message}: {e}")
        finally:
            delattr(self, "conn")

    def run(self, statement):
        """Execute the sql in the database and return the results.

        The results are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """
        # Remove spaces and EOL

        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(";")

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith("\\G"):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()

            attempts = 0
            while attempts in [0, 1]:
                with self.conn.cursor() as cur:
                    try:
                        try:
                            for result in special.execute(cur, sql):
                                yield result
                            break
                        except special.CommandNotFound:  # Regular SQL
                            cur.execute(sql)
                            yield self.get_result(cur)
                            break
                    except EOFError as e:  # User enters `exit`
                        raise e
                    except RequestError as e:
                        logger.error(
                            f"SQL Gateway was timed out. Attempting to reconnect. Attempt {attempts+1}. Error: {e}"
                        )
                        attempts += 1
                        self.reconnect()

    def get_result(self, cursor):
        """Get the current result's data from the cursor."""
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT or SHOW.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            status = format_status(rows_length=len(rows), cursor=cursor)
        else:
            logger.debug("No rows in result.")
            rows = None
            status = format_status(rows_length=None, cursor=cursor)
        return (title, rows, headers, status)

    def _fetch_table_metadata(self, schema=None):
        """Fetch all relations and split into (tables, views) lists.
        :param schema: schema to query, defaults to self.database
        """
        TABLE_NAME = 2
        TABLE_TYPE = 3
        schema = schema or self.database
        with self.conn.cursor() as cur:
            data = cur.tables(schema_name=schema).fetchall()
        tables = [row[TABLE_NAME] for row in data if row[TABLE_TYPE] != "VIEW"]
        views = [row[TABLE_NAME] for row in data if row[TABLE_TYPE] == "VIEW"]
        return tables, views

    def tables(self):
        """Yields table names."""
        tables, _ = self._fetch_table_metadata()
        for row in tables:
            yield (row,)

    def views(self):
        """Yields view names."""
        _, views = self._fetch_table_metadata()
        for row in views:
            yield (row,)

    def table_columns(self, tables, schema=None):
        """Yields column names.
        :param tables: iterable of table names to fetch columns for
        :param schema: schema to query, defaults to self.database
        """
        TABLE_NAME = 2
        COLUMN_NAME = 3
        schema = schema or self.database

        # Build a lookup set that includes both escaped and unescaped names
        # so we can match JDBC results (unescaped) against dbmetadata keys (escaped)
        def _unescape(name):
            if name and len(name) >= 2:
                for ch in ("`", '"'):
                    if name.startswith(ch) and name.endswith(ch):
                        return name[1:-1]
            return name

        raw_tables = {_unescape(t) for t in tables} | set(tables)

        with self.conn.cursor() as cur:
            if len(raw_tables) < 100:
                data = cur.columns(schema_name=schema).fetchall()
                _columns = [(i[TABLE_NAME], i[COLUMN_NAME]) for i in data]
            else:
                _columns = []
                for table in tables:
                    try:
                        data = cur.columns(
                            schema_name=schema, table_name=_unescape(table)
                        ).fetchall()
                        _transformed = [(i[TABLE_NAME], i[COLUMN_NAME]) for i in data]
                        _columns.extend(_transformed)
                    except Exception as e:
                        logger.debug(f"Error fetching columns for {table}: {e}")

        for row in _columns:
            if row[0] in raw_tables:
                yield row[0], row[1]

    def current_catalog(self):
        """Return the name of the currently active catalog."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_catalog()")
            row = cur.fetchone()
            return row[0] if row else ""

    def catalogs(self):
        with self.conn.cursor() as cur:
            return [row[0] for row in cur.catalogs().fetchall()]

    def catalog_schema_map(self):
        """Return {catalog: [schema, ...]} for all accessible catalogs/schemas.

        Uses a single query against information_schema instead of one
        round-trip per catalog.
        """
        result = {}
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT catalog_name, schema_name "
                    "FROM system.information_schema.schemata "
                    "WHERE schema_name != 'information_schema' "
                    "ORDER BY catalog_name, schema_name"
                )
                for row in cur.fetchall():
                    result.setdefault(row[0], []).append(row[1])
        except Exception:
            logger.debug(
                "information_schema query failed, "
                "falling back to per-catalog schema fetch"
            )
            # Fall back to one call per catalog
            for catalog in self.catalogs():
                try:
                    result[catalog] = self._schemas_in_catalog(catalog)
                except Exception:
                    logger.debug("Error fetching schemas for catalog %s", catalog)
        return result

    def catalog_column_map(self):
        """Return {(catalog, schema): [(table_name, column_name), ...]} for all columns.

        Uses a single query against information_schema instead of one
        round-trip per schema.
        """
        result = {}
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT table_catalog, table_schema, table_name, column_name "
                    "FROM system.information_schema.columns "
                    "WHERE table_schema != 'information_schema' "
                    "AND table_name NOT RLIKE '[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}' "
                    "AND table_name NOT RLIKE '^__(apply_changes_storage|materialization)_' "
                    "ORDER BY table_catalog, table_schema, table_name, ordinal_position"
                )
                for row in cur.fetchall():
                    key = (row[0], row[1])
                    result.setdefault(key, []).append((row[2], row[3]))
        except Exception as e:
            logger.debug(
                "information_schema.columns query failed (%s), "
                "falling back to active-schema-only column fetch",
                e,
            )
        logger.debug(
            "catalog_column_map: %d schemas, %d total columns",
            len(result),
            sum(len(v) for v in result.values()),
        )
        return result

    def catalog_table_map(self):
        """Return {(catalog, schema): [(table_name, table_type), ...]} for all tables.

        Uses a single query against information_schema instead of one
        round-trip per schema.  table_type is 'TABLE' or 'VIEW'.
        """
        result = {}
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT table_catalog, table_schema, table_name, table_type "
                    "FROM system.information_schema.tables "
                    "WHERE table_schema != 'information_schema' "
                    "AND table_name NOT RLIKE '[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}' "
                    "AND table_name NOT RLIKE '^__(apply_changes_storage|materialization)_' "
                    "ORDER BY table_catalog, table_schema, table_name"
                )
                for row in cur.fetchall():
                    key = (row[0], row[1])
                    result.setdefault(key, []).append((row[2], row[3]))
        except Exception:
            logger.debug(
                "information_schema.tables query failed, "
                "falling back to active-schema-only table fetch"
            )
        return result

    def _schemas_in_catalog(self, catalog):
        """Return schema names within a specific catalog."""
        SCHEMA_NAME = 0
        with self.conn.cursor() as cur:
            data = cur.schemas(catalog_name=catalog).fetchall()
            return [row[SCHEMA_NAME] for row in data]

    def databases(self):
        with self.conn.cursor() as cur:
            _databases = cur.schemas().fetchall()
            return [x[0] for x in _databases]
