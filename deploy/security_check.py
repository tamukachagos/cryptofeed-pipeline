#!/usr/bin/env python3
"""
Production security pre-flight check.

Run before every deployment:
    python deploy/security_check.py

Exits with code 1 if any FAIL check fires.
Prints a summary table of all checks.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"

results: list[tuple[str, str, str]] = []  # (level, check, message)
failed = False


def check(level: str, name: str, condition: bool, detail: str = "") -> None:
    global failed
    status = PASS if condition else (FAIL if level == "FAIL" else WARN)
    if not condition and level == "FAIL":
        failed = True
    results.append((status, name, detail if not condition else ""))


# ── Environment checks ────────────────────────────────────────────────────────
env = os.getenv("ENVIRONMENT", "development").lower()
check("WARN",  "ENVIRONMENT set",       bool(os.getenv("ENVIRONMENT")),
      "ENVIRONMENT not set — defaulting to development")

bypass = os.getenv("BYPASS_AUTH", "false").lower() == "true"
check("FAIL", "BYPASS_AUTH is false",  not bypass,
      "BYPASS_AUTH=true is set — this disables ALL authentication")

enc_key = os.getenv("SECRET_ENCRYPTION_KEY", "").strip()
check("FAIL", "SECRET_ENCRYPTION_KEY set", bool(enc_key),
      "SECRET_ENCRYPTION_KEY not set — api_secrets will use insecure dev key")

if enc_key:
    dev_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    check("FAIL", "SECRET_ENCRYPTION_KEY is not dev key", enc_key != dev_key,
          "Using insecure all-zero dev key in production")

check("FAIL", "REDIS_PASSWORD set",    bool(os.getenv("REDIS_PASSWORD", "").strip()),
      "REDIS_PASSWORD not set — Redis is unauthenticated")

redis_pw = os.getenv("REDIS_PASSWORD", "")
check("WARN", "REDIS_PASSWORD not trivial", len(redis_pw) >= 16,
      "REDIS_PASSWORD looks too short — use ≥16 chars")

check("FAIL", "STRIPE_WEBHOOK_SECRET set", bool(os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()),
      "STRIPE_WEBHOOK_SECRET not set — billing webhooks disabled")

check("FAIL", "STRIPE_SECRET_KEY set",     bool(os.getenv("STRIPE_SECRET_KEY", "").strip()),
      "STRIPE_SECRET_KEY not set — checkout disabled")

check("FAIL", "INTERNAL_API_SECRET set",   bool(os.getenv("INTERNAL_API_SECRET", "").strip()),
      "INTERNAL_API_SECRET not set — /internal/* returns 503")

enable_docs = os.getenv("ENABLE_DOCS", "false").lower() == "true"
check("FAIL" if env == "production" else "WARN",
      "ENABLE_DOCS is false",  not enable_docs,
      "ENABLE_DOCS=true exposes Swagger UI — disable in production")

check("WARN", "INTERNAL_ALLOWED_IPS set",  bool(os.getenv("INTERNAL_ALLOWED_IPS", "").strip()),
      "INTERNAL_ALLOWED_IPS not set — /internal/* accessible from any IP with correct secret")

# ── Email / CAPTCHA / monitoring checks ───────────────────────────────────────
check("WARN", "SMTP_HOST configured",       bool(os.getenv("SMTP_HOST", "").strip()),
      "SMTP_HOST not set — /billing/request-verify OTP emails cannot be delivered")

check("WARN", "CF_TURNSTILE_SECRET_KEY set", bool(os.getenv("CF_TURNSTILE_SECRET_KEY", "").strip()),
      "CF_TURNSTILE_SECRET_KEY not set — /billing/checkout has no CAPTCHA protection")

check("WARN", "PAGERDUTY_INTEGRATION_KEY set", bool(os.getenv("PAGERDUTY_INTEGRATION_KEY", "").strip()),
      "PAGERDUTY_INTEGRATION_KEY not set — Prometheus alerts will not page on-call")

# PostgreSQL vs SQLite
db_url = os.getenv("DATABASE_URL", "")
if env == "production":
    check("WARN", "DATABASE_URL uses PostgreSQL",
          db_url.startswith("postgresql") or db_url.startswith("postgres"),
          "DATABASE_URL not set to PostgreSQL — SQLite is not recommended for production")

# ── File checks ───────────────────────────────────────────────────────────────
root = Path(__file__).resolve().parents[1]

check("FAIL", "No .env committed",     not (root / ".env").exists(),
      ".env file found in repo root — must not be committed")

check("WARN", "keys.json absent",      not (root / "api" / "keys.json").exists() or
                                        (root / "api" / "keys.json").read_text().strip() in ("{}", ""),
      "api/keys.json exists with data — migrate to SQLite")

check("FAIL", "nginx.conf present",    (root / "deploy" / "nginx.conf").exists(),
      "deploy/nginx.conf missing — TLS not configured")

check("FAIL", "redis.conf present",    (root / "deploy" / "redis.conf").exists(),
      "deploy/redis.conf missing — Redis not hardened")

# ── Code checks ───────────────────────────────────────────────────────────────
try:
    from api.crypto import _ENV, _RAW_KEY, _DEV_KEY
    check("FAIL" if env == "production" else "WARN",
          "Crypto key not dev key (if prod)",
          env != "production" or _RAW_KEY != _DEV_KEY,
          "Production is using the insecure dev encryption key")
except Exception as e:
    check("WARN", "api.crypto importable", False, f"Cannot import api.crypto: {e}")

# ── Print results ─────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print(f"  CryptoFeed Security Pre-Flight  (ENVIRONMENT={env.upper()})")
print("=" * 70)

for status, name, detail in results:
    marker = "✗" if "FAIL" in status else ("⚠" if "WARN" in status else "✓")
    print(f"  {status}  {marker}  {name:<45}  {detail}")

print("=" * 70)
pass_count = sum(1 for s, _, _ in results if "PASS" in s)
fail_count = sum(1 for s, _, _ in results if "FAIL" in s)
warn_count = sum(1 for s, _, _ in results if "WARN" in s)
print(f"  {pass_count} passed  |  {warn_count} warnings  |  {fail_count} failures")
print("=" * 70 + "\n")

if failed:
    print("DEPLOYMENT BLOCKED — fix FAIL items before proceeding.\n")
    sys.exit(1)
elif warn_count:
    print("Deployment permitted with warnings. Address WARN items before go-live.\n")
else:
    print("All checks passed. System is ready for production deployment.\n")
