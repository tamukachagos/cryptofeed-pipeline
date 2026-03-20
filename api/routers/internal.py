"""Internal API endpoints — called server-to-server only (protected by X-Internal-Secret header)."""
from __future__ import annotations

import os
import hashlib
from datetime import datetime, UTC
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from api.db import get_db

INTERNAL_SECRET = os.getenv("INTERNAL_API_SECRET", "")

# Comma-separated list of IPs allowed to call /internal/* (empty = any IP allowed)
_INTERNAL_ALLOWED_IPS_RAW = os.getenv("INTERNAL_ALLOWED_IPS", "")
_INTERNAL_ALLOWED_IPS: set[str] = {
    ip.strip() for ip in _INTERNAL_ALLOWED_IPS_RAW.split(",") if ip.strip()
}

router = APIRouter(prefix="/internal", tags=["internal"])


def _require_secret(secret: Optional[str], request: Optional[Request] = None) -> None:
    if not INTERNAL_SECRET:
        raise HTTPException(status_code=503, detail="Internal API not configured")
    # IP allowlist (if configured)
    if _INTERNAL_ALLOWED_IPS and request is not None:
        client_ip = request.client.host if request.client else ""
        if client_ip not in _INTERNAL_ALLOWED_IPS:
            raise HTTPException(status_code=403, detail="Forbidden")
    if secret != INTERNAL_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")


def _get_db():
    return get_db()


class CreateUserRequest(BaseModel):
    email: str
    plan: str = "free"
    api_key_hash: Optional[str] = None
    api_key_prefix: Optional[str] = None
    stripe_customer_id: Optional[str] = None


@router.get("/users/{email}")
async def get_user(
    email: str,
    request: Request,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret, request)
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
    request: Request,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret, request)
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
    request: Request,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret, request)
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
    request: Request,
    x_internal_secret: Optional[str] = Header(None),
):
    _require_secret(x_internal_secret, request)
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
