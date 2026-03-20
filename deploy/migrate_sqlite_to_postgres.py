#!/usr/bin/env python3
"""
Migrate users.db (SQLite) → PostgreSQL.

Usage:
    DATABASE_URL=postgresql://user:pass@host:5432/cryptofeed \
    DATA_DIR=/data \
    python deploy/migrate_sqlite_to_postgres.py

The script is idempotent — it upserts rows by email, so it is safe to run
multiple times (useful for catch-up runs before the final cutover).

Steps:
  1. Connect to PostgreSQL, create the users table if it does not exist.
  2. Read all rows from SQLite users.db.
  3. Upsert every row into PostgreSQL (ON CONFLICT (email) DO UPDATE SET ...).
  4. Print a summary.

Prerequisites:
    pip install psycopg2-binary
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DATABASE_URL = os.getenv("DATABASE_URL", "")
DATA_DIR     = os.getenv("DATA_DIR", "./data")
SQLITE_PATH  = Path(DATA_DIR) / "users.db"

if not DATABASE_URL:
    print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
    sys.exit(1)

if not SQLITE_PATH.exists():
    print(f"ERROR: SQLite database not found at {SQLITE_PATH}", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# ── PostgreSQL schema ──────────────────────────────────────────────────────────

_CREATE_TABLE_PG = """
CREATE TABLE IF NOT EXISTS users (
    id                    SERIAL PRIMARY KEY,
    email                 TEXT UNIQUE NOT NULL,
    plan                  TEXT NOT NULL DEFAULT 'free',
    api_key_hash          TEXT,
    api_key_prefix        TEXT,
    api_secret            TEXT,
    allowed_ips           TEXT DEFAULT '[]',
    stripe_customer_id    TEXT,
    stripe_session_id     TEXT,
    created_at            TEXT NOT NULL,
    requests_today        INTEGER NOT NULL DEFAULT 0,
    requests_this_month   INTEGER NOT NULL DEFAULT 0,
    last_request_date     TEXT,
    daily_usage           TEXT NOT NULL DEFAULT '0,0,0,0,0,0,0'
);
"""

_UPSERT_SQL = """
INSERT INTO users (
    email, plan, api_key_hash, api_key_prefix, api_secret,
    allowed_ips, stripe_customer_id, stripe_session_id,
    created_at, requests_today, requests_this_month,
    last_request_date, daily_usage
) VALUES (
    %(email)s, %(plan)s, %(api_key_hash)s, %(api_key_prefix)s, %(api_secret)s,
    %(allowed_ips)s, %(stripe_customer_id)s, %(stripe_session_id)s,
    %(created_at)s, %(requests_today)s, %(requests_this_month)s,
    %(last_request_date)s, %(daily_usage)s
)
ON CONFLICT (email) DO UPDATE SET
    plan                = EXCLUDED.plan,
    api_key_hash        = EXCLUDED.api_key_hash,
    api_key_prefix      = EXCLUDED.api_key_prefix,
    api_secret          = EXCLUDED.api_secret,
    allowed_ips         = EXCLUDED.allowed_ips,
    stripe_customer_id  = EXCLUDED.stripe_customer_id,
    stripe_session_id   = EXCLUDED.stripe_session_id,
    requests_today      = EXCLUDED.requests_today,
    requests_this_month = EXCLUDED.requests_this_month,
    last_request_date   = EXCLUDED.last_request_date,
    daily_usage         = EXCLUDED.daily_usage;
"""


def _read_sqlite() -> list[dict]:
    conn = sqlite3.connect(str(SQLITE_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM users").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _row_defaults(row: dict) -> dict:
    """Ensure all expected columns exist (handles older DB schemas)."""
    defaults = {
        "email": "",
        "plan": "free",
        "api_key_hash": None,
        "api_key_prefix": None,
        "api_secret": None,
        "allowed_ips": "[]",
        "stripe_customer_id": None,
        "stripe_session_id": None,
        "created_at": "1970-01-01T00:00:00+00:00",
        "requests_today": 0,
        "requests_this_month": 0,
        "last_request_date": None,
        "daily_usage": "0,0,0,0,0,0,0",
    }
    defaults.update({k: v for k, v in row.items() if k in defaults})
    return defaults


def main() -> None:
    print(f"CryptoFeed — SQLite → PostgreSQL migration")
    print(f"  Source : {SQLITE_PATH}")
    print(f"  Target : {DATABASE_URL[:DATABASE_URL.index('@') + 1]}***")
    print()

    rows = _read_sqlite()
    print(f"  Read {len(rows)} user(s) from SQLite.")

    if not rows:
        print("  Nothing to migrate.")
        return

    pg = psycopg2.connect(DATABASE_URL)
    pg.autocommit = False
    cur = pg.cursor()

    # Create table
    cur.execute(_CREATE_TABLE_PG)
    pg.commit()
    print("  PostgreSQL schema ready.")

    # Upsert rows
    ok = 0
    errors = 0
    for row in rows:
        try:
            cur.execute(_UPSERT_SQL, _row_defaults(row))
            ok += 1
        except Exception as exc:
            pg.rollback()
            print(f"  [ERROR] row email={row.get('email','?')}: {exc}", file=sys.stderr)
            errors += 1

    pg.commit()
    cur.close()
    pg.close()

    print(f"\n  Migrated : {ok}  |  Errors : {errors}")
    if errors:
        print("  WARNING: some rows failed — review errors above.", file=sys.stderr)
        sys.exit(1)
    else:
        print("  Migration complete. Set DATABASE_URL in production and restart services.")


if __name__ == "__main__":
    main()
