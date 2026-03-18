"""API key authentication and plan enforcement."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from fastapi import Header, HTTPException, Request, status

KEYS_FILE = Path(__file__).parent / "keys.json"

PLAN_LIMITS = {
    "free":       {"max_days": 7,   "rate_limit_per_min": 10,   "max_levels": 10},
    "pro":        {"max_days": 90,  "rate_limit_per_min": 120,  "max_levels": 50},
    "enterprise": {"max_days": 730, "rate_limit_per_min": 1000, "max_levels": 400},
}


def _load_keys() -> dict:
    if not KEYS_FILE.exists():
        return {}
    try:
        return json.loads(KEYS_FILE.read_text())
    except Exception:
        return {}


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


async def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> APIKeyInfo:
    """FastAPI dependency: validates API key, returns plan info."""
    keys = _load_keys()
    key_hash = _hash_key(x_api_key)

    if key_hash not in keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key. Get one at https://cryptofeed.io/signup",
        )

    entry = keys[key_hash]
    return APIKeyInfo(key_hash=key_hash, plan=entry["plan"], owner=entry["owner"])


# Optional: allow bypass in dev mode via env var
BYPASS_AUTH = os.getenv("BYPASS_AUTH", "false").lower() == "true"

async def optional_auth(x_api_key: str | None = Header(None, alias="X-API-Key")) -> APIKeyInfo:
    """In BYPASS_AUTH=true mode, returns enterprise plan for any key."""
    if BYPASS_AUTH:
        return APIKeyInfo(key_hash="dev", plan="enterprise", owner="dev")
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    return await require_api_key(x_api_key)
