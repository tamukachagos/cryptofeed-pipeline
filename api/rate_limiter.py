"""Sliding-window rate limiter + daily row quota + scraping detector.

Redis key schema:
    rl:<key_hash>              ZSET  — per-minute request window (UUID → epoch_s)
    quota:rows:<key_hash>:<date> STR  — daily cumulative row count (integer)
    cursor:<key_hash>          ZSET  — cursor usage timestamps for scraping detection

All keys have TTLs; idle keys self-clean.
Falls back to per-process in-memory tracking when Redis is unavailable.
"""
from __future__ import annotations

import os
import time
import uuid
from collections import deque
from datetime import date, timezone

from fastapi import HTTPException, status
from loguru import logger

from api.auth import APIKeyInfo, PLAN_LIMITS

# ── Daily row caps per plan ───────────────────────────────────────────────────
DAILY_ROW_CAPS = {
    "free":       100_000,
    "pro":       5_000_000,
    "enterprise": 0,          # 0 = unlimited
}

# ── Cursor scraping detection ─────────────────────────────────────────────────
# If a key submits >CURSOR_BURST cursor requests in CURSOR_WINDOW_S seconds,
# it is performing sequential pagination scraping.  Impose CURSOR_PENALTY_S delay.
_CURSOR_BURST     = int(os.getenv("CURSOR_BURST_LIMIT", "20"))
_CURSOR_WINDOW_S  = 60.0
_CURSOR_PENALTY_S = int(os.getenv("CURSOR_PENALTY_S", "300"))  # 5 min slow-down

# In-process quota fallback (per-process; resets on restart)
_in_mem_quota: dict[str, int] = {}   # "<key_hash>:<date>" → row count

# ── Redis connection (lazy, shared across workers via module import) ─────────
_redis_client = None
_redis_unavailable = False   # flip once if connection fails; re-try every 60 s
_redis_retry_after: float = 0.0
_REDIS_RETRY_INTERVAL = 60.0

_RL_KEY_TTL = 90   # seconds — Redis key expiry


def _get_redis():
    """Return an async Redis client, or None if unavailable."""
    global _redis_client, _redis_unavailable, _redis_retry_after
    now = time.monotonic()

    if _redis_unavailable and now < _redis_retry_after:
        return None

    if _redis_client is not None:
        return _redis_client

    try:
        import redis.asyncio as aioredis
        host     = os.getenv("REDIS_HOST", "localhost")
        port     = int(os.getenv("REDIS_PORT", 6379))
        password = os.getenv("REDIS_PASSWORD", "") or None
        _redis_client = aioredis.Redis(host=host, port=port, password=password,
                                       decode_responses=True)
        _redis_unavailable = False
        return _redis_client
    except Exception as exc:
        logger.warning(f"[rate_limiter] Redis unavailable — falling back to in-process limiter: {exc}")
        _redis_unavailable = True
        _redis_retry_after = now + _REDIS_RETRY_INTERVAL
        return None


# ── In-process fallback ──────────────────────────────────────────────────────
_windows: dict[str, deque] = {}


def _check_in_memory(key_hash: str, limit: int) -> None:
    now = time.monotonic()
    window = _windows.setdefault(key_hash, deque())
    while window and now - window[0] > 60.0:
        window.popleft()
    if len(window) >= limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded: {limit} req/min (in-process fallback).",
            headers={"Retry-After": "60"},
        )
    window.append(now)


# ── Public interface ─────────────────────────────────────────────────────────
async def check_rate_limit(key_info: APIKeyInfo) -> None:
    """Raise 429 if the key exceeds its plan's per-minute request limit.

    Must be called with ``await`` — uses async Redis pipeline when available.
    """
    limit = PLAN_LIMITS.get(key_info.plan, PLAN_LIMITS["free"])["rate_limit_per_min"]
    redis = _get_redis()

    if redis is None:
        _check_in_memory(key_info.key_hash, limit)
        return

    try:
        rkey = f"rl:{key_info.key_hash}"
        now_s = time.time()
        window_start = now_s - 60.0
        member = str(uuid.uuid4())

        # Atomic pipeline: add current request, trim old entries, count
        async with redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(rkey, "-inf", window_start)
            pipe.zadd(rkey, {member: now_s})
            pipe.zcard(rkey)
            pipe.expire(rkey, _RL_KEY_TTL)
            results = await pipe.execute()

        count = results[2]   # zcard result
        if count > limit:
            # Undo the add we just made so the window stays accurate
            await redis.zrem(rkey, member)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded: {limit} req/min for {key_info.plan} plan.",
                headers={"Retry-After": "60"},
            )
    except HTTPException:
        raise
    except Exception as exc:
        # Redis error mid-request — fall back silently rather than blocking traffic
        logger.warning(f"[rate_limiter] Redis pipeline error — falling back: {exc}")
        global _redis_client
        _redis_client = None           # force reconnect attempt next call
        _check_in_memory(key_info.key_hash, limit)


# ── Daily row quota ───────────────────────────────────────────────────────────

def _quota_key(key_hash: str) -> str:
    today = date.today().isoformat()
    return f"quota:rows:{key_hash}:{today}"


def _seconds_until_midnight() -> int:
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    midnight = (now + _dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int((midnight - now).total_seconds())


async def check_daily_quota(key_info: APIKeyInfo) -> None:
    """Raise 429 if the key has exceeded its daily row quota.

    Called at the START of each data route handler (before fetching Parquet).
    """
    cap = DAILY_ROW_CAPS.get(key_info.plan, DAILY_ROW_CAPS["free"])
    if cap == 0:
        return  # unlimited plan

    rkey  = _quota_key(key_info.key_hash)
    redis = _get_redis()

    try:
        if redis is not None:
            used = await redis.get(rkey)
            used_count = int(used or 0)
        else:
            used_count = _in_mem_quota.get(rkey, 0)
    except Exception:
        return  # fail open on Redis error — don't block traffic

    if used_count >= cap:
        ttl = _seconds_until_midnight()
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"Daily row quota exceeded ({cap:,} rows/day for {key_info.plan} plan). "
                f"Resets in {ttl // 3600}h {(ttl % 3600) // 60}m (UTC midnight)."
            ),
            headers={"Retry-After": str(ttl), "X-Quota-Limit": str(cap)},
        )


async def record_row_usage(key_info: APIKeyInfo, rows: int) -> None:
    """Increment the daily row counter for a key after returning data.

    Called at the END of each data route handler, after df.head(limit).
    """
    if rows <= 0:
        return
    cap = DAILY_ROW_CAPS.get(key_info.plan, DAILY_ROW_CAPS["free"])
    if cap == 0:
        return  # unlimited

    rkey  = _quota_key(key_info.key_hash)
    redis = _get_redis()

    try:
        if redis is not None:
            async with redis.pipeline(transaction=False) as pipe:
                pipe.incrby(rkey, rows)
                pipe.expire(rkey, 90_000)  # 25 hours — covers timezone drift
                await pipe.execute()
        else:
            _in_mem_quota[rkey] = _in_mem_quota.get(rkey, 0) + rows
    except Exception:
        pass  # fail open on Redis error


# ── Cursor scraping detection ─────────────────────────────────────────────────

async def check_cursor_pattern(key_info: APIKeyInfo, cursor: str | None) -> None:
    """Detect and throttle sequential cursor-pagination scraping patterns.

    A client performing automated bulk extraction will make many paginated
    requests with successive cursor values in a short window. This function
    detects that pattern and imposes a progressive delay.
    """
    if not cursor:
        return  # First page — not a scraping signal

    redis = _get_redis()
    if redis is None:
        return  # Cannot detect without Redis; skip

    rkey   = f"cursor:{key_info.key_hash}"
    now_s  = time.time()
    window_start = now_s - _CURSOR_WINDOW_S

    try:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(rkey, "-inf", window_start)
            pipe.zadd(rkey, {str(uuid.uuid4()): now_s})
            pipe.zcard(rkey)
            pipe.expire(rkey, int(_CURSOR_WINDOW_S) + 10)
            results = await pipe.execute()

        cursor_count = results[2]
        if cursor_count > _CURSOR_BURST:
            logger.warning(
                f"[rate_limiter] Scraping pattern detected: key={key_info.key_hash[:16]} "
                f"made {cursor_count} cursor requests in {_CURSOR_WINDOW_S:.0f}s"
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Automated pagination scraping detected. "
                    f"Retry after {_CURSOR_PENALTY_S}s. "
                    f"For bulk data access, contact support for an enterprise plan."
                ),
                headers={"Retry-After": str(_CURSOR_PENALTY_S)},
            )
    except HTTPException:
        raise
    except Exception:
        pass  # fail open
