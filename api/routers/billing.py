"""Billing router — Stripe checkout and webhook.

Endpoints:
    POST /billing/checkout          Create a Stripe checkout session
    POST /billing/webhook           Stripe webhook (generates API key on payment)
    GET  /billing/success?session_id= Show API key after payment
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

import stripe
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel

router = APIRouter(prefix="/billing", tags=["billing"])

# ── Stripe config ─────────────────────────────────────────────────────────────
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET  = os.getenv("STRIPE_WEBHOOK_SECRET", "")

PRICE_IDS = {
    "pro":        os.getenv("STRIPE_PRO_PRICE_ID", ""),
    "enterprise": os.getenv("STRIPE_ENTERPRISE_PRICE_ID", ""),
}

PLAN_NAMES = {
    "pro":        "CryptoDataAPI Pro",
    "enterprise": "CryptoDataAPI Enterprise",
}

# ── Keys store ────────────────────────────────────────────────────────────────
KEYS_FILE = Path(__file__).resolve().parents[1] / "keys.json"


def _load_keys() -> dict:
    if not KEYS_FILE.exists():
        return {}
    return json.loads(KEYS_FILE.read_text())


def _save_keys(data: dict) -> None:
    KEYS_FILE.write_text(json.dumps(data, indent=2))


def _generate_key(plan: str, owner: str, stripe_customer_id: str = "") -> str:
    """Generate a new API key, store its hash, return the raw key."""
    key = f"cda_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    data = _load_keys()
    data[key_hash] = {
        "plan": plan,
        "owner": owner,
        "stripe_customer_id": stripe_customer_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_keys(data)
    logger.success(f"[billing] API key generated for {owner} ({plan})")
    return key


# ── Pending sessions store (in-memory, simple) ────────────────────────────────
# Maps session_id → raw API key so the success page can show it
_pending: dict[str, dict] = {}


# ── Endpoints ─────────────────────────────────────────────────────────────────
class CheckoutRequest(BaseModel):
    plan: str   # "pro" | "enterprise"
    email: str


@router.post("/checkout")
async def create_checkout(req: CheckoutRequest):
    """Create a Stripe checkout session and return the URL."""
    plan = req.plan.lower()
    if plan not in PRICE_IDS:
        raise HTTPException(status_code=400, detail=f"Invalid plan: {plan}. Use 'pro' or 'enterprise'.")

    price_id = PRICE_IDS[plan]
    if not price_id:
        raise HTTPException(status_code=500, detail="Stripe price ID not configured.")

    if not stripe.api_key:
        raise HTTPException(status_code=500, detail="Stripe not configured.")

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
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except stripe.errors.SignatureVerificationError:
        logger.warning("[billing] Invalid webhook signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        plan    = session.get("metadata", {}).get("plan", "pro")
        email   = session.get("customer_email") or session.get("metadata", {}).get("email", "")
        customer_id = session.get("customer", "")
        session_id  = session.get("id", "")

        # Generate API key
        api_key = _generate_key(plan, email, customer_id)
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        key_prefix = api_key[:12] + "..."

        # Persist user in SQLite via internal router
        try:
            from api.routers.internal import _get_db
            conn = _get_db()
            conn.execute(
                """INSERT OR REPLACE INTO users
                   (email, plan, api_key_hash, api_key_prefix, stripe_customer_id, created_at,
                    requests_today, requests_this_month, daily_usage)
                   VALUES (?, ?, ?, ?, ?, ?, 0, 0, '0,0,0,0,0,0,0')""",
                (email, plan, key_hash, key_prefix, customer_id,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            conn.close()
        except Exception as db_err:
            logger.error(f"[billing] DB write failed: {db_err}")

        # Store for success page retrieval
        _pending[session_id] = {
            "api_key": api_key,
            "plan": plan,
            "email": email,
        }

        logger.success(f"[billing] Payment complete for {email} ({plan}) → key issued")

    return JSONResponse({"status": "ok"})


@router.get("/success")
async def payment_success(session_id: str):
    """Return the API key after successful payment."""
    if session_id not in _pending:
        # Try to look up from Stripe directly (in case server restarted)
        try:
            session = stripe.checkout.Session.retrieve(session_id)
            if session.payment_status == "paid":
                plan  = session.metadata.get("plan", "pro")
                email = session.customer_email or session.metadata.get("email", "")
                # Check if key already exists for this customer
                data = _load_keys()
                existing = next(
                    (v for v in data.values() if v.get("owner") == email and v.get("plan") == plan),
                    None,
                )
                if existing:
                    return {"status": "already_issued", "message": "Key already sent. Check your email."}
                api_key = _generate_key(plan, email, session.customer or "")
                _pending[session_id] = {"api_key": api_key, "plan": plan, "email": email}
            else:
                raise HTTPException(status_code=404, detail="Payment not completed.")
        except stripe.StripeError:
            raise HTTPException(status_code=404, detail="Session not found.")

    result = _pending[session_id]
    return {
        "status": "success",
        "api_key": result["api_key"],
        "plan": result["plan"],
        "email": result["email"],
        "message": f"Save this key — it will not be shown again.",
        "docs": "https://cryptodataapi.dev/docs",
    }
