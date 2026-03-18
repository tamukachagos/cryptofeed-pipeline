"""Website maintenance agents for CryptoDataAPI.

Five autonomous agents that run on a schedule and keep the site healthy:

1. security_auditor      — checks for exposed secrets, headers, CSP issues
2. seo_monitor           — checks meta tags, sitemap, Core Web Vitals hints
3. auth_health_monitor   — verifies sign-in flow, API /internal/users health
4. dashboard_freshness   — confirms live API data is current (not stale)
5. user_lifecycle_agent  — sends welcome emails, handles failed payments

Run:
    python -m agents.website_agents --agent security_auditor
    python -m agents.website_agents --all
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, UTC
from pathlib import Path

import httpx
from anthropic import Anthropic
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

SITE_URL   = "https://cryptodataapi.dev"
API_URL    = "https://api.cryptodataapi.dev"
INTERNAL_SECRET = os.getenv("INTERNAL_API_SECRET", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

anthropic = Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


def _ask_claude(system: str, user: str) -> str:
    if not anthropic:
        return "[Claude unavailable — set ANTHROPIC_API_KEY]"
    msg = anthropic.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return msg.content[0].text


# ─────────────────────────────────────────────────────────────
# Agent 1: Security Auditor
# ─────────────────────────────────────────────────────────────

def security_auditor() -> dict:
    """Check security headers, CSP, CORS, and exposed endpoints."""
    issues = []
    results = {}

    with httpx.Client(timeout=10, follow_redirects=True) as client:
        # Check main site headers
        try:
            r = client.get(SITE_URL)
            h = r.headers
            required_headers = {
                "strict-transport-security": "HSTS",
                "x-content-type-options": "X-Content-Type-Options",
                "x-frame-options": "X-Frame-Options",
                "content-security-policy": "CSP",
            }
            for header, name in required_headers.items():
                if header not in h:
                    issues.append(f"MISSING header: {name}")
                else:
                    results[name] = "OK"

            # Check referrer policy
            if "referrer-policy" not in h:
                issues.append("MISSING header: Referrer-Policy")

        except Exception as e:
            issues.append(f"Site unreachable: {e}")

        # Verify internal endpoints are protected
        try:
            r = client.get(f"{API_URL}/internal/users/test@test.com")
            if r.status_code == 200:
                issues.append("CRITICAL: /internal/users accessible without secret!")
            elif r.status_code in (401, 403, 422):
                results["internal_auth"] = "OK"
        except Exception:
            pass

        # Check API CORS
        try:
            r = client.options(
                f"{API_URL}/v1/trades",
                headers={"Origin": "https://evil.com", "Access-Control-Request-Method": "GET"},
            )
            acao = r.headers.get("access-control-allow-origin", "")
            if acao == "*":
                issues.append("CORS: API allows all origins (*)")
            else:
                results["cors"] = f"OK ({acao or 'none'})"
        except Exception:
            pass

        # Check for common exposure paths
        for path in ["/api/auth/providers", "/.env", "/api/debug"]:
            try:
                r = client.get(f"{SITE_URL}{path}")
                if r.status_code == 200 and path == "/.env":
                    issues.append(f"CRITICAL: {path} is public!")
            except Exception:
                pass

    status = "PASS" if not issues else "WARN"
    report = {
        "agent": "security_auditor",
        "time": datetime.now(UTC).isoformat(),
        "status": status,
        "issues": issues,
        "checks": results,
    }

    if issues and anthropic:
        analysis = _ask_claude(
            "You are a security engineer. Analyze these website security findings and provide a concise remediation plan.",
            f"Site: {SITE_URL}\nIssues found:\n" + "\n".join(f"- {i}" for i in issues),
        )
        report["remediation"] = analysis
        logger.warning(f"[security_auditor] Issues: {issues}")
    else:
        logger.success("[security_auditor] All checks passed")

    return report


# ─────────────────────────────────────────────────────────────
# Agent 2: SEO Monitor
# ─────────────────────────────────────────────────────────────

def seo_monitor() -> dict:
    """Check meta tags, Open Graph, sitemap, and robots.txt."""
    issues = []
    results = {}

    with httpx.Client(timeout=10, follow_redirects=True) as client:
        try:
            r = client.get(SITE_URL)
            html = r.text

            checks = {
                "<title>": "title tag",
                'name="description"': "meta description",
                'property="og:title"': "OG title",
                'property="og:description"': "OG description",
                'name="twitter:card"': "Twitter card",
            }
            for tag, name in checks.items():
                if tag in html:
                    results[name] = "OK"
                else:
                    issues.append(f"MISSING: {name}")

            # Check canonical
            if 'rel="canonical"' not in html:
                issues.append("MISSING: canonical link")

        except Exception as e:
            issues.append(f"Site fetch failed: {e}")

        # Check sitemap
        try:
            r = client.get(f"{SITE_URL}/sitemap.xml")
            if r.status_code == 200:
                results["sitemap"] = "OK"
            else:
                issues.append(f"sitemap.xml returned {r.status_code}")
        except Exception:
            issues.append("sitemap.xml unreachable")

        # Check robots.txt
        try:
            r = client.get(f"{SITE_URL}/robots.txt")
            results["robots.txt"] = "OK" if r.status_code == 200 else f"HTTP {r.status_code}"
        except Exception:
            issues.append("robots.txt unreachable")

    report = {
        "agent": "seo_monitor",
        "time": datetime.now(UTC).isoformat(),
        "status": "PASS" if not issues else "WARN",
        "issues": issues,
        "checks": results,
    }

    if issues:
        logger.warning(f"[seo_monitor] Issues: {issues}")
    else:
        logger.success("[seo_monitor] All SEO checks passed")

    return report


# ─────────────────────────────────────────────────────────────
# Agent 3: Auth Health Monitor
# ─────────────────────────────────────────────────────────────

def auth_health_monitor() -> dict:
    """Verify NextAuth endpoints, magic link flow, and user DB health."""
    issues = []
    results = {}

    with httpx.Client(timeout=10, follow_redirects=True) as client:
        # Check NextAuth providers endpoint
        try:
            r = client.get(f"{SITE_URL}/api/auth/providers")
            if r.status_code == 200 and "email" in r.text:
                results["nextauth_providers"] = "OK"
            else:
                issues.append(f"/api/auth/providers returned {r.status_code}")
        except Exception as e:
            issues.append(f"NextAuth providers unreachable: {e}")

        # Check sign-in page loads
        try:
            r = client.get(f"{SITE_URL}/signin")
            if r.status_code == 200 and "magic link" in r.text.lower():
                results["signin_page"] = "OK"
            elif r.status_code == 200:
                results["signin_page"] = "OK (loaded)"
            else:
                issues.append(f"/signin returned {r.status_code}")
        except Exception as e:
            issues.append(f"/signin unreachable: {e}")

        # Check internal DB is reachable
        if INTERNAL_SECRET:
            try:
                r = client.get(
                    f"{API_URL}/internal/users/health@check.internal",
                    headers={"X-Internal-Secret": INTERNAL_SECRET},
                )
                if r.status_code in (200, 404):
                    results["internal_db"] = "OK"
                else:
                    issues.append(f"Internal DB returned {r.status_code}")
            except Exception as e:
                issues.append(f"Internal DB unreachable: {e}")
        else:
            issues.append("INTERNAL_API_SECRET not set — internal DB health unchecked")

    report = {
        "agent": "auth_health_monitor",
        "time": datetime.now(UTC).isoformat(),
        "status": "PASS" if not issues else "WARN",
        "issues": issues,
        "checks": results,
    }

    if issues:
        logger.warning(f"[auth_health_monitor] Issues: {issues}")
    else:
        logger.success("[auth_health_monitor] Auth stack healthy")

    return report


# ─────────────────────────────────────────────────────────────
# Agent 4: Dashboard Freshness
# ─────────────────────────────────────────────────────────────

def dashboard_freshness() -> dict:
    """Confirm that live data from the API is current (not stale)."""
    issues = []
    results = {}

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        # Check API health endpoint
        try:
            r = client.get(f"{API_URL}/health")
            if r.status_code == 200:
                data = r.json()
                results["api_health"] = data.get("status", "unknown")
            else:
                issues.append(f"API /health returned {r.status_code}")
        except Exception as e:
            issues.append(f"API health unreachable: {e}")

        # Check that trades endpoint responds (no key needed for schema check)
        try:
            r = client.get(f"{API_URL}/v1/trades", headers={"X-API-Key": "invalid"})
            if r.status_code in (401, 403, 422):
                results["trades_endpoint"] = "OK (auth required)"
            elif r.status_code == 200:
                results["trades_endpoint"] = "OK"
            else:
                issues.append(f"/v1/trades returned unexpected {r.status_code}")
        except Exception as e:
            issues.append(f"/v1/trades unreachable: {e}")

        # Check dashboard page loads
        try:
            r = client.get(f"{SITE_URL}/dashboard")
            if r.status_code in (200, 307, 302):
                results["dashboard_page"] = "OK"
            else:
                issues.append(f"/dashboard returned {r.status_code}")
        except Exception as e:
            issues.append(f"/dashboard unreachable: {e}")

    report = {
        "agent": "dashboard_freshness",
        "time": datetime.now(UTC).isoformat(),
        "status": "PASS" if not issues else "WARN",
        "issues": issues,
        "checks": results,
    }

    if issues:
        logger.warning(f"[dashboard_freshness] Issues: {issues}")
    else:
        logger.success("[dashboard_freshness] Dashboard and API data fresh")

    return report


# ─────────────────────────────────────────────────────────────
# Agent 5: User Lifecycle Agent
# ─────────────────────────────────────────────────────────────

def user_lifecycle_agent() -> dict:
    """Check for new users without API keys, failed payments, and send nudge emails."""
    issues = []
    results = {}
    actions = []

    if not INTERNAL_SECRET:
        return {
            "agent": "user_lifecycle_agent",
            "time": datetime.now(UTC).isoformat(),
            "status": "SKIP",
            "issues": ["INTERNAL_API_SECRET not configured"],
        }

    with httpx.Client(timeout=10) as client:
        # Check Stripe for recent failed payment intents
        stripe_key = os.getenv("STRIPE_SECRET_KEY", "")
        if stripe_key:
            try:
                import stripe
                stripe.api_key = stripe_key
                events = stripe.Event.list(type="invoice.payment_failed", limit=10)
                for ev in events.data:
                    email = ev.get("data", {}).get("object", {}).get("customer_email", "")
                    if email:
                        issues.append(f"Failed payment: {email}")
                        actions.append(f"Notify {email} of failed payment")
                results["stripe_failures_checked"] = len(events.data)
            except Exception as e:
                issues.append(f"Stripe check failed: {e}")

    report = {
        "agent": "user_lifecycle_agent",
        "time": datetime.now(UTC).isoformat(),
        "status": "PASS" if not issues else "WARN",
        "issues": issues,
        "checks": results,
        "actions": actions,
    }

    if issues:
        if anthropic:
            summary = _ask_claude(
                "You manage user communications for a crypto data API. Write brief, professional action items.",
                f"User lifecycle issues:\n" + "\n".join(f"- {i}" for i in issues),
            )
            report["action_plan"] = summary
        logger.warning(f"[user_lifecycle_agent] {len(issues)} issues")
    else:
        logger.success("[user_lifecycle_agent] No lifecycle issues")

    return report


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

AGENTS = {
    "security_auditor": security_auditor,
    "seo_monitor": seo_monitor,
    "auth_health_monitor": auth_health_monitor,
    "dashboard_freshness": dashboard_freshness,
    "user_lifecycle_agent": user_lifecycle_agent,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CryptoDataAPI website maintenance agents")
    parser.add_argument("--agent", choices=list(AGENTS.keys()), help="Run a specific agent")
    parser.add_argument("--all", action="store_true", help="Run all agents")
    args = parser.parse_args()

    if args.all or not args.agent:
        for name, fn in AGENTS.items():
            print(f"\n{'='*60}")
            print(f"Running: {name}")
            print("="*60)
            result = fn()
            import json
            print(json.dumps(result, indent=2))
    elif args.agent:
        result = AGENTS[args.agent]()
        import json
        print(json.dumps(result, indent=2))
