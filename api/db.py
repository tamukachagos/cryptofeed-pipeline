"""Database abstraction — SQLite (dev) or PostgreSQL (production).

Set DATABASE_URL=postgresql://user:pass@host:5432/dbname to use PostgreSQL.
Leave unset to use SQLite at DATA_DIR/users.db.

All callers use ? parameter placeholders (SQLite style). This module
translates them to %s for PostgreSQL automatically, and handles
INSERT OR REPLACE / INSERT OR IGNORE → ON CONFLICT clauses.

Usage:
    from api.db import get_db

    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        conn.commit()
    finally:
        conn.close()
"""
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any

DATABASE_URL = os.getenv("DATABASE_URL", "")
_IS_POSTGRES = DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres")
_SQLITE_PATH = Path(os.getenv("DATA_DIR", "./data")) / "users.db"

# ── Schema definitions ─────────────────────────────────────────────────────────

_USERS_DDL_SQLITE = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        plan TEXT NOT NULL DEFAULT 'free',
        api_key_hash TEXT,
        api_key_prefix TEXT,
        api_secret TEXT,
        allowed_ips TEXT DEFAULT '[]',
        stripe_customer_id TEXT,
        stripe_session_id TEXT,
        created_at TEXT NOT NULL,
        requests_today INTEGER NOT NULL DEFAULT 0,
        requests_this_month INTEGER NOT NULL DEFAULT 0,
        last_request_date TEXT,
        daily_usage TEXT NOT NULL DEFAULT '0,0,0,0,0,0,0'
    )
"""

_USERS_DDL_PG = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        plan TEXT NOT NULL DEFAULT 'free',
        api_key_hash TEXT,
        api_key_prefix TEXT,
        api_secret TEXT,
        allowed_ips TEXT DEFAULT '[]',
        stripe_customer_id TEXT,
        stripe_session_id TEXT,
        created_at TEXT NOT NULL,
        requests_today INTEGER NOT NULL DEFAULT 0,
        requests_this_month INTEGER NOT NULL DEFAULT 0,
        last_request_date TEXT,
        daily_usage TEXT NOT NULL DEFAULT '0,0,0,0,0,0,0'
    )
"""

# ── SQL translation (SQLite → PostgreSQL) ─────────────────────────────────────

_PH_RE = re.compile(r"\?")
_OR_REPLACE_RE = re.compile(r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)", re.IGNORECASE)
_OR_IGNORE_RE  = re.compile(r"INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)",  re.IGNORECASE)


def _build_upsert(sql: str, conflict_col: str = "email") -> str:
    """Append ON CONFLICT ... DO UPDATE SET <all non-conflict cols> = EXCLUDED.<col>."""
    m = re.search(r"INSERT\s+INTO\s+\w+\s*\(([^)]+)\)", sql, re.IGNORECASE)
    if not m:
        # Can't parse column list — fall back to no-op update
        return sql.rstrip().rstrip(";") + f" ON CONFLICT ({conflict_col}) DO UPDATE SET id = id"
    cols = [c.strip() for c in m.group(1).split(",")]
    update_cols = [c for c in cols if c != conflict_col]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return sql.rstrip().rstrip(";") + f" ON CONFLICT ({conflict_col}) DO UPDATE SET {set_clause}"


def _translate(sql: str) -> str:
    """Convert SQLite SQL syntax to PostgreSQL SQL syntax."""
    if _OR_REPLACE_RE.search(sql):
        m = _OR_REPLACE_RE.search(sql)
        table = m.group(1)
        sql = _OR_REPLACE_RE.sub(f"INSERT INTO {table}", sql, count=1)
        sql = _PH_RE.sub("%s", sql)
        return _build_upsert(sql)
    if _OR_IGNORE_RE.search(sql):
        m = _OR_IGNORE_RE.search(sql)
        table = m.group(1)
        sql = _OR_IGNORE_RE.sub(f"INSERT INTO {table}", sql, count=1)
        sql = _PH_RE.sub("%s", sql)
        return sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    return _PH_RE.sub("%s", sql)


# ── PostgreSQL wrapper ─────────────────────────────────────────────────────────

class _PgCursor:
    """Wraps a psycopg2 RealDictCursor so .fetchone()/.fetchall() return dict-like rows."""
    def __init__(self, cursor: Any) -> None:
        self._cur = cursor

    def fetchone(self):
        return self._cur.fetchone()  # RealDictRow — supports row["col"]

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount


class _PgConn:
    """Wraps a psycopg2 connection with a sqlite3-compatible interface."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        import psycopg2.extras
        self._conn.cursor_factory = psycopg2.extras.RealDictCursor

    def execute(self, sql: str, params: tuple = ()) -> _PgCursor:
        cur = self._conn.cursor()
        cur.execute(_translate(sql), params)
        return _PgCursor(cur)

    def executemany(self, sql: str, params_seq) -> None:
        cur = self._conn.cursor()
        cur.executemany(_translate(sql), params_seq)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


# ── Schema initialisation ──────────────────────────────────────────────────────

def _init_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute(_USERS_DDL_SQLITE)
    conn.commit()
    for col, defn in [
        ("stripe_session_id", "TEXT"),
        ("api_secret",        "TEXT"),
        ("allowed_ips",       "TEXT DEFAULT '[]'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists


def _init_pg(conn: _PgConn) -> None:
    conn.execute(_USERS_DDL_PG)
    # ADD COLUMN IF NOT EXISTS requires PostgreSQL 9.6+
    for col, defn in [
        ("stripe_session_id", "TEXT"),
        ("api_secret",        "TEXT"),
        ("allowed_ips",       "TEXT DEFAULT '[]'"),
    ]:
        conn.execute(
            f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} {defn}"
        )
    conn.commit()


# ── Public factory ─────────────────────────────────────────────────────────────

def get_db():
    """Open and return a database connection.

    Returns either a sqlite3.Connection (SQLite) or _PgConn (PostgreSQL).
    Both support .execute(sql, params), .commit(), .close() and dict-style
    row access.  Caller must call .close() when done (or use a try/finally).
    """
    if _IS_POSTGRES:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        pg = _PgConn(conn)
        _init_pg(pg)
        return pg
    else:
        _SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(_SQLITE_PATH))
        conn.row_factory = sqlite3.Row
        _init_sqlite(conn)
        return conn
