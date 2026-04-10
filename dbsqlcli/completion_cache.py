"""Persist and restore completer metadata to/from disk.

Completions are cached per workspace hostname so that they are available
immediately on startup while a background refresh fetches fresh data.
"""

import hashlib
import json
import logging
import os

LOGGER = logging.getLogger(__name__)

CACHE_DIR = os.path.expanduser("~/.dbsqlcli/cache")

# Keys we serialize from / restore to a DBSQLCompleter instance
_STATE_KEYS = (
    "databases",
    "catalogs",
    "catalog_schemas",
    "current_catalog",
    "dbname",
    "dbmetadata",
)


def _cache_path(hostname):
    """Return the cache file path for a given workspace hostname."""
    slug = hashlib.sha256(hostname.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{slug}.json")


def save(completer, hostname):
    """Serialize completer state to disk."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        state = {key: getattr(completer, key) for key in _STATE_KEYS}
        path = _cache_path(hostname)
        with open(path, "w") as f:
            json.dump(state, f, separators=(",", ":"))
        LOGGER.debug("Saved completion cache to %s", path)
    except Exception:
        LOGGER.debug("Failed to save completion cache", exc_info=True)


def load(completer, hostname):
    """Restore completer state from disk. Returns True if cache was loaded."""
    path = _cache_path(hostname)
    try:
        with open(path) as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    except Exception:
        LOGGER.debug("Failed to load completion cache", exc_info=True)
        return False

    try:
        for key in _STATE_KEYS:
            if key in state:
                setattr(completer, key, state[key])

        # Rebuild the all_completions set from restored metadata
        all_c = set(completer.keywords + completer.functions)
        all_c.update(completer.databases)
        all_c.update(completer.catalogs)
        for kind in ("tables", "views", "functions"):
            for schema_tables in completer.dbmetadata.get(kind, {}).values():
                if isinstance(schema_tables, dict):
                    all_c.update(schema_tables.keys())
                    for cols in schema_tables.values():
                        if isinstance(cols, list):
                            all_c.update(cols)
        completer.all_completions = all_c
        LOGGER.debug("Loaded completion cache from %s", path)
        return True
    except Exception:
        LOGGER.debug("Failed to apply completion cache", exc_info=True)
        completer.reset_completions()
        return False
