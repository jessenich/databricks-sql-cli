import re
import threading
from collections import OrderedDict

from dbsqlcli.completer import DBSQLCompleter
from dbsqlcli.sqlexecute import SQLExecute
from dbsqlcli.packages.special.main import COMMANDS

import logging

LOGGER = logging.getLogger(__name__)

# Matches table names containing a UUID anywhere (e.g. DLT event logs,
# materialization tables like __materialization_mat_<uuid>_fact_f)
_UUID_RE = re.compile(
    r"[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}",
    re.IGNORECASE,
)


class CompletionRefresher(object):

    refreshers = OrderedDict()

    def __init__(self):
        self._completer_thread = None
        self._restart_refresh = threading.Event()

    def refresh(self, executor, callbacks, completer_options=None):
        """Creates a SQLCompleter object and populates it with the relevant
        completion suggestions in a background thread.

        executor - SQLExecute object, used to extract the credentials to connect
                   to the database.
        callbacks - A function or a list of functions to call after the thread
                    has completed the refresh. The newly created completion
                    object will be passed in as an argument to each callback.
        completer_options - dict of options to pass to SQLCompleter.
        """
        if completer_options is None:
            completer_options = {}

        if self.is_refreshing():
            self._restart_refresh.set()
            return [(None, None, None, "Auto-completion refresh restarted.")]
        else:
            self._completer_thread = threading.Thread(
                target=self._bg_refresh,
                args=(executor, callbacks, completer_options),
                name="completion_refresh",
                daemon=True,
            )
            self._completer_thread.start()
            return [
                (None, None, None, "Auto-completion refresh started in the background.")
            ]

    def is_refreshing(self):
        return self._completer_thread and self._completer_thread.is_alive()

    def _bg_refresh(self, sqlexecute, callbacks, completer_options):
        completer = DBSQLCompleter(**completer_options)

        # Create a new pgexecute method to popoulate the completions.
        e = sqlexecute
        executor = SQLExecute(
            hostname=e.hostname,
            http_path=e.http_path,
            access_token=e.access_token,
            database=e.database,
            auth_type=e.auth_type,
        )

        # If callbacks is a single function then push it into a list.
        if callable(callbacks):
            callbacks = [callbacks]

        while 1:
            for name, refresher in self.refreshers.items():
                try:
                    refresher(completer, executor)
                except Exception as e:
                    LOGGER.debug("Error in refresher %s: %s", name, e)
                if self._restart_refresh.is_set():
                    self._restart_refresh.clear()
                    break
            else:
                # Break out of while loop if the for loop finishes natually
                # without hitting the break statement.
                break

            # Start over the refresh from the beginning if the for loop hit the
            # break statement.
            continue

        # Log diagnostic info about what was loaded
        LOGGER.debug(
            "Refresh complete: catalog=%r, dbname=%r, "
            "catalogs=%d, databases=%d, catalog_schemas=%d, "
            "metadata_keys=%r",
            completer.current_catalog,
            completer.dbname,
            len(completer.catalogs),
            len(completer.databases),
            len(completer.catalog_schemas),
            list(completer.dbmetadata["tables"].keys()),
        )

        for callback in callbacks:
            callback(completer)


def refresher(name, refreshers=CompletionRefresher.refreshers):
    """Decorator to add the decorated function to the dictionary of
    refreshers. Any function decorated with a @refresher will be executed as
    part of the completion refresh routine."""

    def wrapper(wrapped):
        refreshers[name] = wrapped
        return wrapped

    return wrapper


@refresher("catalogs")
def refresh_catalogs(completer, executor):
    # Set the current catalog so metadata keys can be catalog-qualified
    try:
        completer.set_catalog(executor.current_catalog())
    except Exception:
        LOGGER.debug("Could not determine current catalog")

    # Fetch all catalog→schemas in a single query instead of N round-trips
    try:
        csm = executor.catalog_schema_map()
        completer.extend_catalog_names(list(csm.keys()))
        for catalog, schemas in csm.items():
            completer.extend_catalog_schemas(catalog, schemas)
    except Exception:
        LOGGER.debug("Could not fetch catalog/schema map")


@refresher("databases")
def refresh_databases(completer, executor):
    completer.extend_database_names(executor.databases())


@refresher("schemata")
def refresh_schemata(completer, executor):
    # Register schemas with catalog qualification for each known catalog,
    # so that "catalog.schema." lookups use the correct metadata key.
    for catalog, schemas in completer.catalog_schemas.items():
        for schema in schemas:
            completer.extend_schemata(schema, catalog=catalog)

    # Also register the active schema under the current catalog
    completer.extend_schemata(executor.database)
    completer.set_dbname(executor.database)


@refresher("tables")
def refresh_tables(completer, executor):
    # Try to fetch ALL table names across all catalogs in one query.
    # This populates table/view completions for every catalog.schema combo.
    catalog_table_data = executor.catalog_table_map()

    if catalog_table_data:
        for (catalog, schema), entries in catalog_table_data.items():
            saved_catalog = completer.current_catalog
            saved_dbname = completer.dbname
            completer.set_catalog(catalog)
            completer.set_dbname(schema)
            tables = [e[0] for e in entries if e[1] != "VIEW" and not _UUID_RE.search(e[0])]
            views = [e[0] for e in entries if e[1] == "VIEW" and not _UUID_RE.search(e[0])]
            completer.extend_relations(((t,) for t in tables), kind="tables")
            completer.extend_relations(((v,) for v in views), kind="views")
            completer.set_catalog(saved_catalog)
            completer.set_dbname(saved_dbname)
    else:
        # Fallback: fetch tables/views for the active schema only
        tables, views = executor._fetch_table_metadata()
        tables = [t for t in tables if not _UUID_RE.search(t)]
        views = [v for v in views if not _UUID_RE.search(v)]
        completer.extend_relations(((t,) for t in tables), kind="tables")
        completer.extend_relations(((v,) for v in views), kind="views")

    # Load columns for the active schema only (loading all would be too slow)
    catalog_key = (
        f"{completer.current_catalog}.{executor.database}"
        if completer.current_catalog
        else executor.database
    )
    current_tables = completer.dbmetadata["tables"].get(catalog_key, {}).keys()
    if not current_tables:
        current_tables = (
            completer.dbmetadata["tables"].get(executor.database, {}).keys()
        )
    completer.extend_columns(executor.table_columns(current_tables), kind="tables")

    current_views = completer.dbmetadata["views"].get(catalog_key, {}).keys()
    if not current_views:
        current_views = completer.dbmetadata["views"].get(executor.database, {}).keys()
    if current_views:
        completer.extend_columns(executor.table_columns(current_views), kind="views")


@refresher("special_commands")
def refresh_special(completer, executor):
    completer.extend_special_commands(COMMANDS.keys())
