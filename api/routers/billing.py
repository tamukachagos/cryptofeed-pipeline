"""Billing router — Stripe checkout and webhook.

Endpoints:
    POST /billing/request-verify    Send OTP email for checkout verification
    POST /billing/verify-email      Confirm OTP → return short-lived verification_token
    POST /billing/checkout          Create Stripe session (requires verification_token)
    POST /billing/webhook           Stripe webhook (generates API key on payment)
    GET  /billing/success?session_id= Show API key after payment (consume-once)
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import redis as _redis_sync
import stripe
from api.crypto import encrypt_secret
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel

# ── Cloudflare Turnstile ───────────────────────────────────────────────────────
_TURNSTILE_SECRET = os.getenv("CF_TURNSTILE_SECRET_KEY", "")
_TURNSTILE_URL    = "https://challenges.cloudflare.com/turnstile/v0/siteverify"

# ── Redis (for OTP + verification token storage) ──────────────────────────────
def _get_redis() -> _redis_sync.Redis:
    return _redis_sync.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        password=os.getenv("REDIS_PASSWORD", "") or None,
        decode_responses=True,
        socket_timeout=3,
    )

_OTP_TTL_S         = 600   # 10 minutes
_VERIFY_TOKEN_TTL  = 1800  # 30 minutes (must complete checkout within this window)
_OTP_RATE_KEY      = "otp_rate:{}"   # per-email OTP request rate limit

router = APIRouter(prefix="/billing", tags=["billing"])

# ── Stripe config ─────────────────────────────────────────────────────────────
stripe.api_key      = os.getenv("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET      = os.getenv("STRIPE_WEBHOOK_SECRET", "")

PRICE_IDS = {
    "pro":        os.getenv("STRIPE_PRO_PRICE_ID", ""),
    "enterprise": os.getenv("STRIPE_ENTERPRISE_PRICE_ID", ""),
}

PLAN_NAMES = {
    "pro":        "CryptoDataAPI Pro",
    "enterprise": "CryptoDataAPI Enterprise",
}

# Warn at startup — do not raise so import works in dev without vars set
if not WEBHOOK_SECRET:
    logger.warning(
        "[billing] STRIPE_WEBHOOK_SECRET is not set. "
        "Webhook endpoint will reject ALL incoming webhooks. "
        "Set this env var in production."
    )
if not stripe.api_key:
    logger.warning("[billing] STRIPE_SECRET_KEY is not set. Checkout will be unavailable.")


# ── Key generation (SQLite-backed, idempotent) ────────────────────────────────

def _generate_key(plan: str, owner: str, stripe_customer_id: str = "",
                  stripe_session_id: str = "") -> tuple[str, str]:
    """
    Generate a new (api_key, api_secret) pair and persist to SQLite.

    Returns:
        (api_key, api_secret) — both shown to user once.
        ("", "") if stripe_session_id already issued (idempotency).

    Design:
        api_key    = public identifier   — sent in X-API-Key header
        api_secret = private signing key — used for HMAC, never transmitted
    """
    from api.routers.internal import _get_db
    conn = _get_db()

    try:
        # Idempotency: check for existing session
        if stripe_session_id:
            row = conn.execute(
                "SELECT email FROM users WHERE stripe_session_id = ?",
                (stripe_session_id,)
            ).fetchone()
            if row:
                logger.warning(
                    f"[billing] Duplicate webhook for session {stripe_session_id[:16]}… — skipping"
                )
                return "", ""

        api_key    = f"cda_{secrets.token_urlsafe(32)}"
        api_secret = secrets.token_urlsafe(48)
        key_hash   = hashlib.sha256(api_key.encode()).hexdigest()
        key_prefix = api_key[:12] + "..."
        now        = datetime.now(timezone.utc).isoformat()
        # Encrypt api_secret before storage — plaintext never touches disk
        encrypted_secret = encrypt_secret(api_secret)

        conn.execute(
            """INSERT OR REPLACE INTO users
               (email, plan, api_key_hash, api_key_prefix, api_secret,
                stripe_customer_id, stripe_session_id, created_at,
                requests_today, requests_this_month, daily_usage)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '0,0,0,0,0,0,0')""",
            (owner, plan, key_hash, key_prefix, encrypted_secret,
             stripe_customer_id, stripe_session_id, now),
        )
        conn.commit()
    finally:
        conn.close()

    logger.success(f"[billing] API key generated for {owner} ({plan})")
    from api.auth import invalidate_keys_cache
    invalidate_keys_cache()
    return api_key, api_secret


def _key_exists_for_email(email: str) -> bool:
    """Return True if a non-null api_key_hash already exists for this email."""
    from api.routers.internal import _get_db
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT api_key_hash FROM users WHERE email = ? AND api_key_hash IS NOT NULL",
            (email,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


# ── Pending sessions store (in-memory, TTL-bounded) ──────────────────────────
# Maps session_id → {api_key, api_secret, plan, email, expires_at}
# Credentials are shown once, expire after 15 minutes, then are purged.
_pending: dict[str, dict] = {}
_PENDING_TTL_S = 900  # 15 minutes


def _pending_cleanup() -> None:
    """Remove expired pending entries."""
    import time
    now = time.monotonic()
    expired = [sid for sid, v in _pending.items() if v.get("expires_at", 0) < now]
    for sid in expired:
        del _pending[sid]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _verify_turnstile(token: str, remote_ip: str = "") -> bool:
    """Call Cloudflare Turnstile siteverify API. Returns True on success."""
    if not _TURNSTILE_SECRET:
        return True  # CAPTCHA not configured — allow (dev mode)
    try:
        data = urllib.parse.urlencode({
            "secret":   _TURNSTILE_SECRET,
            "response": token,
            "remoteip": remote_ip,
        }).encode()
        req = urllib.request.Request(_TURNSTILE_URL, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read())
        return bool(result.get("success"))
    except Exception as exc:
        logger.warning(f"[billing] Turnstile verify error: {exc}")
        return False  # fail open only in dev; see below


def _send_otp(email: str, otp: str, plan: str) -> None:
    """Send OTP via SMTP. Silently logs on failure to avoid leaking SMTP errors to clients."""
    try:
        from api.email_utils import send_otp
        send_otp(email, otp, plan)
    except Exception as exc:
        logger.error(f"[billing] OTP email send failed for {email}: {exc}")


# ── Endpoints ─────────────────────────────────────────────────────────────────

class VerifyEmailRequest(BaseModel):
    email: str
    plan:  str = "pro"


class ConfirmEmailRequest(BaseModel):
    email: str
    otp:   str


class CheckoutRequest(BaseModel):
    plan:               str   # "pro" | "enterprise"
    email:              str
    verification_token: str   # returned by /verify-email
    cf_turnstile_token: str = ""  # Cloudflare Turnstile response token


_checkout_windows: dict[str, list] = {}
_CHECKOUT_LIMIT = 5   # max 5 checkout sessions per IP per 10 minutes
_CHECKOUT_WINDOW = 600.0


def _checkout_rate_limit(ip: str) -> None:
    import time
    now = time.monotonic()
    window = _checkout_windows.setdefault(ip, [])
    _checkout_windows[ip] = [t for t in window if now - t < _CHECKOUT_WINDOW]
    if len(_checkout_windows[ip]) >= _CHECKOUT_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Too many checkout requests. Please wait before trying again.",
            headers={"Retry-After": "600"},
        )
    _checkout_windows[ip].append(now)


@router.post("/request-verify")
async def request_email_verify(req: VerifyEmailRequest):
    """Send a one-time passcode to the given email address.

    Rate-limited to 3 OTP requests per email per hour to prevent abuse.
    """
    plan = req.plan.lower()
    if plan not in ("pro", "enterprise"):
        raise HTTPException(status_code=400, detail="Invalid plan.")

    # Normalise email (basic)
    email = req.email.strip().lower()
    if "@" not in email or len(email) > 254:
        raise HTTPException(status_code=400, detail="Invalid email address.")

    try:
        r = _get_redis()
        rate_key = _OTP_RATE_KEY.format(hashlib.sha256(email.encode()).hexdigest()[:16])
        count = r.incr(rate_key)
        if count == 1:
            r.expire(rate_key, 3600)  # 1-hour window
        if count > 3:
            raise HTTPException(
                status_code=429,
                detail="Too many verification requests for this email. Wait 1 hour.",
                headers={"Retry-After": "3600"},
            )

        otp = f"{secrets.randbelow(900000) + 100000}"  # 6-digit
        otp_key = f"otp:{hashlib.sha256(email.encode()).hexdigest()}"
        r.setex(otp_key, _OTP_TTL_S, otp)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[billing] Redis error in request-verify: {exc}")
        raise HTTPException(status_code=503, detail="Verification service temporarily unavailable.")

    _send_otp(email, otp, plan)
    # Always return 200 (don't reveal whether email is registered)
    return {"status": "ok", "message": "Verification code sent. Check your inbox."}


@router.post("/verify-email")
async def verify_email(req: ConfirmEmailRequest):
    """Verify the OTP and return a short-lived verification_token for checkout.

    The token is valid for 30 minutes and can only be used once.
    """
    email = req.email.strip().lower()
    otp   = req.otp.strip()

    if len(otp) != 6 or not otp.isdigit():
        raise HTTPException(status_code=400, detail="OTP must be a 6-digit number.")

    try:
        r = _get_redis()
        otp_key = f"otp:{hashlib.sha256(email.encode()).hexdigest()}"
        stored  = r.get(otp_key)
    except Exception as exc:
        logger.error(f"[billing] Redis error in verify-email: {exc}")
        raise HTTPException(status_code=503, detail="Verification service temporarily unavailable.")

    if not stored or stored != otp:
        raise HTTPException(status_code=400, detail="Invalid or expired verification code.")

    # Consume the OTP (single-use)
    r.delete(otp_key)

    # Issue a verification token scoped to this email
    vtoken = secrets.token_urlsafe(32)
    vtoken_key = f"vtoken:{vtoken}"
    r.setex(vtoken_key, _VERIFY_TOKEN_TTL, email)

    return {"status": "ok", "verification_token": vtoken,
            "expires_in": _VERIFY_TOKEN_TTL,
            "message": "Email verified. Use verification_token in /billing/checkout."}


@router.post("/checkout")
async def create_checkout(req: CheckoutRequest, request: Request):
    """Create a Stripe checkout session and return the URL.

    Requires a valid verification_token from /billing/verify-email.
    If CF_TURNSTILE_SECRET_KEY is configured, also requires cf_turnstile_token.
    """
    ip = request.client.host if request.client else "unknown"
    _checkout_rate_limit(ip)

    # ── Cloudflare Turnstile CAPTCHA ─────────────────────────────────────────
    if _TURNSTILE_SECRET:
        if not req.cf_turnstile_token:
            raise HTTPException(status_code=400, detail="CAPTCHA token required.")
        if not _verify_turnstile(req.cf_turnstile_token, ip):
            raise HTTPException(status_code=400, detail="CAPTCHA verification failed.")

    # ── Email verification token ─────────────────────────────────────────────
    try:
        r = _get_redis()
        vtoken_key = f"vtoken:{req.verification_token}"
        verified_email = r.get(vtoken_key)
    except Exception as exc:
        logger.error(f"[billing] Redis error in checkout: {exc}")
        raise HTTPException(status_code=503, detail="Verification service temporarily unavailable.")

    if not verified_email:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired verification_token. Request a new code via /billing/request-verify.",
        )
    if verified_email.lower() != req.email.strip().lower():
        raise HTTPException(status_code=400, detail="Email does not match verification token.")

    # Consume the verification token (single-use)
    r.delete(vtoken_key)

    if not stripe.api_key:
        raise HTTPException(status_code=503, detail="Payment service not configured.")
    plan = req.plan.lower()
    if plan not in PRICE_IDS:
        raise HTTPException(status_code=400, detail=f"Invalid plan: {plan}. Use 'pro' or 'enterprise'.")

    price_id = PRICE_IDS[plan]
    if not price_id:
        raise HTTPException(status_code=500, detail="Stripe price ID not configured.")

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            customer_email=req.email,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url="https://cryptodataapi.dev/success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://cryptodataapi.dev/pricing",
            metadata={"plan": plan, "email": req.email},
        )
        return {"checkout_url": session.url, "session_id": session.id}
    except stripe.StripeError as e:
        logger.error(f"[billing] Stripe error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook — generate API key on successful payment."""
    if not WEBHOOK_SECRET:
        logger.error("[billing] Webhook received but STRIPE_WEBHOOK_SECRET is not set — rejecting")
        raise HTTPException(status_code=503, detail="Webhook endpoint not configured")

    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except stripe.errors.SignatureVerificationError:
        logger.warning("[billing] Invalid webhook signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    if event["type"] == "checkout.session.completed":
        session     = event["data"]["object"]
        plan        = session.get("metadata", {}).get("plan", "pro")
        email       = session.get("customer_email") or session.get("metadata", {}).get("email", "")
        customer_id = session.get("customer", "")
        session_id  = session.get("id", "")

        api_key, api_secret = _generate_key(
            plan, email, customer_id, stripe_session_id=session_id
        )
        if not api_key:
            return JSONResponse({"status": "ok", "note": "already_issued"})

        _pending_cleanup()
        _pending[session_id] = {
            "api_key":    api_key,
            "api_secret": api_secret,
            "plan":       plan,
            "email":      email,
            "expires_at": time.monotonic() + _PENDING_TTL_S,
        }
        logger.success(f"[billing] Payment complete for {email} ({plan}) → key issued")

    return JSONResponse({"status": "ok"})


@router.get("/success")
async def payment_success(session_id: str):
    """Return the API key and secret after successful payment (shown once, 15-min window)."""
    _pending_cleanup()
    # Reject empty or suspiciously short session IDs to prevent enumeration
    if not session_id or len(session_id) < 20:
        raise HTTPException(status_code=400, detail="Invalid session_id")

    if session_id not in _pending:
        try:
            session = stripe.checkout.Session.retrieve(session_id)
            if session.payment_status == "paid":
                plan  = session.metadata.get("plan", "pro")
                email = session.customer_email or session.metadata.get("email", "")
                if _key_exists_for_email(email):
                    return {"status": "already_issued",
                            "message": "Key already issued. Check your email."}
                api_key, api_secret = _generate_key(
                    plan, email, session.customer or "", stripe_session_id=session_id
                )
                _pending[session_id] = {
                    "api_key":    api_key,
                    "api_secret": api_secret,
                    "plan":       plan,
                    "email":      email,
                    "expires_at": time.monotonic() + _PENDING_TTL_S,
                }
            else:
                raise HTTPException(status_code=404, detail="Payment not completed.")
        except stripe.StripeError:
            raise HTTPException(status_code=404, detail="Session not found.")

    # Consume once — remove from pending so the credentials can't be retrieved again
    result = _pending.pop(session_id)
    return {
        "status":     "success",
        "api_key":    result["api_key"],
        "api_secret": result["api_secret"],
        "plan":       result["plan"],
        "email":      result["email"],
        "message":    "Save both values — they will not be shown again. Use api_key in X-API-Key and api_secret for HMAC signing.",
        "docs":       "https://cryptodataapi.dev/docs",
    }
