"""
Shared PostgreSQL connection helper for all store modules.

Every module that needs a database connection imports from here instead of
duplicating _conninfo / _connect / register_vector boilerplate.
"""

from __future__ import annotations

import logging
import os
import re

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("intro_agents.db_connection")

_pgvector_registered: set[int] = set()


def conninfo() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Set DATABASE_URL (or HIRING_INTEL_DATABASE_URL) to a PostgreSQL connection string."
        )
    return url


def redacted_conninfo() -> str:
    raw = conninfo()
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", raw, count=1)


def connect(*, register_vector_ext: bool = False) -> psycopg.Connection:
    """
    Open a new psycopg connection with dict_row factory.

    register_vector_ext: if True, register pgvector types on the connection
    (required only for modules that read/write vector columns).
    """
    connection = psycopg.connect(conninfo(), row_factory=dict_row)
    if register_vector_ext:
        conn_id = id(connection)
        if conn_id not in _pgvector_registered:
            from pgvector.psycopg import register_vector
            register_vector(connection)
            _pgvector_registered.add(conn_id)
    return connection
