"""API key authentication and plan enforcement.

Key lookup uses the SQLite users.db (same DB as billing/internal).
A 30-second TTL cache avoids a DB hit on every request.
"""
from __future__ import annotations

import hashlib
import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import Request

from fastapi import Header, HTTPException, status
from loguru import logger

from api.db import get_db

PLAN_LIMITS = {
    "free":       {"max_days": 7,   "rate_limit_per_min": 10,   "max_levels": 10,  "max_rows": 10_000},
    "pro":        {"max_days": 90,  "rate_limit_per_min": 120,  "max_levels": 50,  "max_rows": 100_000},
    "enterprise": {"max_days": 730, "rate_limit_per_min": 1000, "max_levels": 400, "max_rows": 500_000},
}

# ── Key cache (key_hash → {"plan": str, "email": str}) ───────────────────────
_KEYS_CACHE: dict[str, dict] = {}
_KEYS_CACHE_TS: float = 0.0
_KEYS_CACHE_TTL: float = 30.0  # seconds


def _load_keys() -> dict[str, dict]:
    """Return {key_hash: {plan, email}} from the database, with 30s TTL cache."""
    global _KEYS_CACHE, _KEYS_CACHE_TS
    now = time.monotonic()
    if _KEYS_CACHE and (now - _KEYS_CACHE_TS) < _KEYS_CACHE_TTL:
        return _KEYS_CACHE

    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT api_key_hash, plan, email FROM users WHERE api_key_hash IS NOT NULL"
        ).fetchall()
        conn.close()
        _KEYS_CACHE = {row["api_key_hash"]: {"plan": row["plan"], "email": row["email"]}
                       for row in rows}
        _KEYS_CACHE_TS = now
        return _KEYS_CACHE
    except Exception:
        return {}


def invalidate_keys_cache() -> None:
    """Call after writing a new key so the next request picks it up immediately."""
    global _KEYS_CACHE_TS
    _KEYS_CACHE_TS = 0.0


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


class APIKeyInfo:
    def __init__(self, key_hash: str, plan: str, owner: str):
        self.key_hash = key_hash
        self.plan = plan
        self.owner = owner
        self.limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])

    @property
    def max_days(self) -> int:
        return self.limits["max_days"]

    @property
    def max_levels(self) -> int:
        return self.limits["max_levels"]

    @property
    def max_rows(self) -> int:
        return self.limits["max_rows"]


async def require_api_key(
    x_api_key: str = Header(..., alias="X-API-Key"),
    request: "Request | None" = None,
) -> APIKeyInfo:
    """FastAPI dependency: validates API key, returns plan info."""
    keys = _load_keys()
    key_hash = _hash_key(x_api_key)

    if key_hash not in keys:
        # Log with client IP for brute-force detection
        client_ip = ""
        if request is not None and hasattr(request, "client") and request.client:
            client_ip = request.client.host
        logger.warning(
            f"[auth] Invalid API key attempt | ip={client_ip} | "
            f"key_prefix={x_api_key[:8]}..."
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key. Get one at https://cryptodataapi.dev/signup",
        )

    entry = keys[key_hash]
    return APIKeyInfo(key_hash=key_hash, plan=entry["plan"], owner=entry["email"])


# Optional: allow bypass in dev mode via env var
BYPASS_AUTH = os.getenv("BYPASS_AUTH", "false").lower() == "true"


async def optional_auth(x_api_key: str | None = Header(None, alias="X-API-Key")) -> APIKeyInfo:
    """In BYPASS_AUTH=true mode, returns enterprise plan for any key."""
    if BYPASS_AUTH:
        return APIKeyInfo(key_hash="dev", plan="enterprise", owner="dev")
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    return await require_api_key(x_api_key)
