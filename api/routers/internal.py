"""Internal API endpoints — called server-to-server only (protected by X-Internal-Secret header)."""
from __future__ import annotations

import os
import sqlite3
import hashlib
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

INTERNAL_SECRET = os.getenv("INTERNAL_API_SECRET", "")
DB_PATH = Path(os.getenv("DATA_DIR", "./data")) / "users.db"

router = APIRouter(prefix="/internal", tags=["internal"])


def _require_secret(secret: Optional[str]) -> None:
    if not INTERNAL_SECRET:
        raise HTTPException(status_code=503, detail="Internal API not configured")
    if secret != INTERNAL_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")


def _get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            plan TEXT NOT NULL DEFAULT 'free',
            api_key_hash TEXT,
            api_key_prefix TEXT,
            stripe_customer_id TEXT,
            created_at TEXT NOT NULL,
            requests_today INTEGER NOT NULL DEFAULT 0,
            requests_this_month INTEGER NOT NULL DEFAULT 0,
            last_request_date TEXT,
            daily_usage TEXT NOT NULL DEFAULT '0,0,0,0,0,0,0'
        )
    """)
    conn.commit()
    return conn


class CreateUserRequest(BaseModel):
    email: str
    plan: str = "free"
    api_key_hash: Optional[str] = None
    api_key_prefix: Optional[str] = None
    stripe_customer_id: Optional[str] = None


@router.get("/users/{email}")
async def get_user(
    email: str,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret)
    conn = _get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        return dict(row)
    finally:
        conn.close()


@router.post("/users", status_code=201)
async def create_user(
    body: CreateUserRequest,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret)
    conn = _get_db()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO users
               (email, plan, api_key_hash, api_key_prefix, stripe_customer_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                body.email,
                body.plan,
                body.api_key_hash,
                body.api_key_prefix,
                body.stripe_customer_id,
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE email = ?", (body.email,)).fetchone()
        return dict(row)
    finally:
        conn.close()


@router.patch("/users/{email}")
async def update_user(
    email: str,
    body: dict,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret)
    allowed = {"plan", "api_key_hash", "api_key_prefix", "stripe_customer_id"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    conn = _get_db()
    try:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(
            f"UPDATE users SET {set_clause} WHERE email = ?",
            (*updates.values(), email),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        return dict(row)
    finally:
        conn.close()


@router.get("/users/{email}/stats")
async def get_user_stats(
    email: str,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret)
    conn = _get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        d = dict(row)
        daily = [int(x) for x in d.get("daily_usage", "0,0,0,0,0,0,0").split(",")]
        return {
            "plan": d["plan"],
            "api_key_prefix": d.get("api_key_prefix") or "",
            "requests_today": d.get("requests_today", 0),
            "requests_this_month": d.get("requests_this_month", 0),
            "rate_limit": {"free": 10, "pro": 120, "enterprise": 1000}.get(d["plan"], 10),
            "daily_usage": daily,
        }
    finally:
        conn.close()
