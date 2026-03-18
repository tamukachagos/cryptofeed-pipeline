"""Sliding-window in-memory rate limiter per API key hash."""
from __future__ import annotations

import time
from collections import deque

from fastapi import HTTPException, status

from api.auth import APIKeyInfo, PLAN_LIMITS

_windows: dict[str, deque] = {}


def check_rate_limit(key_info: APIKeyInfo) -> None:
    """Raise 429 if the key exceeds its plan's per-minute request limit."""
    limit = PLAN_LIMITS.get(key_info.plan, PLAN_LIMITS["free"])["rate_limit_per_min"]
    now = time.monotonic()
    window = _windows.setdefault(key_info.key_hash, deque())

    # Evict timestamps older than 60 seconds
    while window and now - window[0] > 60.0:
        window.popleft()

    if len(window) >= limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded: {limit} requests/minute for {key_info.plan} plan.",
            headers={"Retry-After": "60"},
        )

    window.append(now)
