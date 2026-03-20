"""
HMAC-SHA256 Request Signing — production-grade authentication.

Two-credential model:
    api_key    — public identifier, sent in X-API-Key (never secret)
    api_secret — private signing key, used locally for HMAC, never transmitted

Headers required for signed requests:
    X-API-Key:   <api_key>
    X-Timestamp: <unix_ms>
    X-Nonce:     <uuid4>
    X-Signature: <HMAC-SHA256(api_secret, canonical_string)>

Canonical string:
    "{METHOD}\n{path}[?query]\n{timestamp_ms}\n{nonce}\n{sha256(body)}"

Security properties:
    - Replay prevention: nonces stored in Redis (NONCE_TTL_S seconds, distributed)
    - Clock skew: ±MAX_CLOCK_SKEW_S (default 30s)
    - Secret never transmitted: only HMAC digest over the wire
    - IP binding: optional per-key allowlist stored in users.db

Backwards compatibility:
    HMAC_REQUIRED=false (default): falls back to bare X-API-Key auth
    HMAC_REQUIRED=true:  enforces signed requests for all keys
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import time
from collections import deque
from pathlib import Path
from typing import Optional

from fastapi import Header, HTTPException, Request, status

from api.auth import _hash_key, APIKeyInfo, PLAN_LIMITS, DB_PATH
from api.crypto import decrypt_secret, is_encrypted

# ── Configuration ─────────────────────────────────────────────────────────────
HMAC_REQUIRED    = os.getenv("HMAC_REQUIRED", "false").lower() == "true"
MAX_CLOCK_SKEW_S = int(os.getenv("HMAC_MAX_CLOCK_SKEW_S", "30"))
NONCE_TTL_S      = MAX_CLOCK_SKEW_S * 2 + 5


# ── Nonce deduplication (Redis-backed, per-process fallback) ─────────────────
_nonce_window: deque[tuple[float, str]] = deque()
_nonce_set:    set[str]                 = set()


def _evict_stale_nonces() -> None:
    now = time.monotonic()
    while _nonce_window and _nonce_window[0][0] < now:
        _, old_nonce = _nonce_window.popleft()
        _nonce_set.discard(old_nonce)


async def _consume_nonce_redis(nonce: str) -> bool:
    """Return True (fresh) using Redis SET NX EX. False = replay.

    Redis is the authoritative nonce store. The in-process fallback is used
    only when Redis is genuinely unavailable — it logs a WARNING each time
    so operators know distributed replay protection is degraded.
    """
    from loguru import logger as _logger
    try:
        from api.rate_limiter import _get_redis
        r = _get_redis()
        if r is not None:
            result = await r.set(f"nonce:{nonce}", 1, ex=NONCE_TTL_S, nx=True)
            return result is not None  # None = key existed = replay
    except Exception as exc:
        _logger.warning(
            f"[hmac_auth] Redis nonce store unavailable ({exc}) — "
            f"falling back to per-process deque. Replay protection is NOT distributed."
        )
    # Fallback to in-process deque (single-worker safety only)
    _evict_stale_nonces()
    if nonce in _nonce_set:
        return False
    expiry = time.monotonic() + NONCE_TTL_S
    _nonce_window.append((expiry, nonce))
    _nonce_set.add(nonce)
    return True


# ── Canonical string builder ─────────────────────────────────────────────────

def _canonical(method: str, path: str, timestamp_ms: str, nonce: str, body: bytes) -> str:
    body_hash = hashlib.sha256(body).hexdigest()
    return f"{method.upper()}\n{path}\n{timestamp_ms}\n{nonce}\n{body_hash}"


def _compute_signature(api_secret: str, canonical: str) -> str:
    return hmac.new(
        api_secret.encode(),
        canonical.encode(),
        hashlib.sha256,
    ).hexdigest()


# ── SQLite helpers ────────────────────────────────────────────────────────────

def _get_key_entry(key_hash: str) -> dict | None:
    """
    Look up api_secret and allowed_ips for a key_hash from users.db.
    Returns None if key not found.
    """
    if not Path(DB_PATH).exists():
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT plan, email, api_secret, allowed_ips FROM users WHERE api_key_hash = ?",
            (key_hash,)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        raw_secret = row["api_secret"] or ""
        # Decrypt if stored encrypted; accept legacy plaintext rows (migration path)
        if raw_secret and is_encrypted(raw_secret):
            try:
                raw_secret = decrypt_secret(raw_secret)
            except ValueError:
                raw_secret = ""  # decryption failure = treat as no secret
        return {
            "plan":        row["plan"],
            "owner":       row["email"],
            "api_secret":  raw_secret,
            "allowed_ips": json.loads(row["allowed_ips"] or "[]"),
        }
    except Exception:
        return None


# ── FastAPI dependency ────────────────────────────────────────────────────────

async def require_signed_request(
    request:     Request,
    x_api_key:   str = Header(..., alias="X-API-Key"),
    x_timestamp: str = Header(..., alias="X-Timestamp"),
    x_nonce:     str = Header(..., alias="X-Nonce"),
    x_signature: str = Header(..., alias="X-Signature"),
) -> APIKeyInfo:
    """FastAPI dependency: validates HMAC-signed request."""

    # 1. Timestamp validation
    try:
        req_ms = int(x_timestamp)
    except ValueError:
        raise HTTPException(status_code=400, detail="X-Timestamp must be Unix milliseconds")

    now_ms = int(time.time() * 1000)
    if abs(now_ms - req_ms) > MAX_CLOCK_SKEW_S * 1000:
        raise HTTPException(
            status_code=401,
            detail=f"Timestamp outside ±{MAX_CLOCK_SKEW_S}s window "
                   f"(server={now_ms}ms, request={req_ms}ms)"
        )

    # 2. Nonce deduplication (distributed via Redis)
    if not await _consume_nonce_redis(x_nonce):
        raise HTTPException(status_code=401, detail="Duplicate nonce — replay attack rejected")

    # 3. Load key record from DB
    key_hash = _hash_key(x_api_key)
    entry    = _get_key_entry(key_hash)
    if entry is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # 4. Verify HMAC signature using stored api_secret
    api_secret = entry.get("api_secret")
    if not api_secret:
        raise HTTPException(
            status_code=401,
            detail="This key has no HMAC secret. Re-generate your key to use signed requests."
        )

    body       = await request.body()
    path       = request.url.path
    if request.url.query:
        path += f"?{request.url.query}"
    canonical  = _canonical(request.method, path, x_timestamp, x_nonce, body)
    expected   = _compute_signature(api_secret, canonical)

    if not hmac.compare_digest(x_signature.lower(), expected.lower()):
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    # 5. IP binding check
    allowed_ips = entry.get("allowed_ips", [])
    if allowed_ips:
        client_ip = request.client.host if request.client else ""
        if client_ip not in allowed_ips:
            raise HTTPException(
                status_code=403,
                detail=f"IP {client_ip} not on allowlist for this key"
            )

    return APIKeyInfo(key_hash=key_hash, plan=entry["plan"], owner=entry["owner"])


async def flexible_auth(
    request:     Request,
    x_api_key:   Optional[str] = Header(None, alias="X-API-Key"),
    x_timestamp: Optional[str] = Header(None, alias="X-Timestamp"),
    x_nonce:     Optional[str] = Header(None, alias="X-Nonce"),
    x_signature: Optional[str] = Header(None, alias="X-Signature"),
) -> APIKeyInfo:
    """
    Use HMAC auth if HMAC_REQUIRED=true or signing headers present.
    Otherwise fall back to bare key auth.
    """
    if HMAC_REQUIRED or (x_timestamp and x_nonce and x_signature):
        if not all([x_api_key, x_timestamp, x_nonce, x_signature]):
            raise HTTPException(
                status_code=401,
                detail="HMAC auth requires X-API-Key, X-Timestamp, X-Nonce, X-Signature"
            )
        return await require_signed_request(request, x_api_key, x_timestamp, x_nonce, x_signature)

    from api.auth import require_api_key
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    return await require_api_key(x_api_key)


# ── Key binding helpers ────────────────────────────────────────────────────────

def bind_ip_to_key(key_hash: str, ip: str) -> bool:
    """Add an IP to the allowlist for a key. Returns True on success."""
    if not Path(DB_PATH).exists():
        return False
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT allowed_ips FROM users WHERE api_key_hash = ?", (key_hash,)
        ).fetchone()
        if row is None:
            conn.close()
            return False
        ips = json.loads(row["allowed_ips"] or "[]")
        if ip not in ips:
            ips.append(ip)
        conn.execute(
            "UPDATE users SET allowed_ips = ? WHERE api_key_hash = ?",
            (json.dumps(ips), key_hash)
        )
        conn.commit()
        conn.close()
        from api.auth import invalidate_keys_cache
        invalidate_keys_cache()
        return True
    except Exception:
        return False


def set_max_concurrent(key_hash: str, max_connections: int) -> bool:
    """Store max concurrent WebSocket connections for a key (metadata only)."""
    if not Path(DB_PATH).exists():
        return False
    try:
        conn = sqlite3.connect(str(DB_PATH))
        # max_concurrent not in schema — store in a JSON metadata column if added
        # For now this is a no-op stub; add 'metadata TEXT' column to extend
        conn.close()
        return True
    except Exception:
        return False
