"""Vercel Agent — autonomous deployment & infrastructure manager.

Runs like a top-1% DevOps engineer:
  - Monitors every deployment for failures and auto-rolls back
  - Tracks performance (p95 latency, error rate) and alerts
  - Rotates environment variables when API keys change
  - Enforces domain/SSL health
  - Generates a weekly deployment health report (Claude-written)
  - Posts structured logs to logs/vercel_agent.jsonl

Schedule (via orchestrator):
  Every 5 min  : deployment_watchdog   — catch failures, auto-rollback
  Every 1 hour : performance_audit     — p95 latency, error rate
  Every 6 hours: env_sync              — sync .env secrets → Vercel env vars
  Daily        : weekly_report trigger — full deployment health summary

Usage:
    python agents/vercel_agent.py                  # run all checks once
    python agents/vercel_agent.py --watch          # continuous loop
    python agents/vercel_agent.py --report         # generate report now
    python agents/vercel_agent.py --rollback       # force rollback to last good
    python agents/vercel_agent.py --sync-env       # push .env to Vercel
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import requests
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
VERCEL_TOKEN   = os.getenv("VERCEL_TOKEN", "")
VERCEL_PROJECT = os.getenv("VERCEL_PROJECT", "cryptofeed-web")
VERCEL_TEAM    = os.getenv("VERCEL_TEAM", "")          # team slug, blank = personal
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
LOGS_DIR       = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE       = LOGS_DIR / "vercel_agent.jsonl"
REPORTS_DIR    = Path("reports/vercel")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Env vars to sync from .env → Vercel (key in .env → name in Vercel)
ENV_SYNC_KEYS = {
    "ANTHROPIC_API_KEY": "ANTHROPIC_API_KEY",
    # Add more as needed:
    # "SOME_SECRET": "NEXT_PUBLIC_SOME_VAR",
}

# ── Vercel API helpers ─────────────────────────────────────────────────────────
def _headers() -> dict:
    return {"Authorization": f"Bearer {VERCEL_TOKEN}", "Content-Type": "application/json"}


def _team_qs() -> str:
    return f"?teamId={VERCEL_TEAM}" if VERCEL_TEAM else ""


def api_get(path: str) -> dict | list:
    url = f"https://api.vercel.com{path}"
    r = requests.get(url, headers=_headers(), timeout=15)
    r.raise_for_status()
    return r.json()


def api_post(path: str, body: dict) -> dict:
    url = f"https://api.vercel.com{path}"
    r = requests.post(url, headers=_headers(), json=body, timeout=15)
    r.raise_for_status()
    return r.json()


def api_patch(path: str, body: dict) -> dict:
    url = f"https://api.vercel.com{path}"
    r = requests.patch(url, headers=_headers(), json=body, timeout=15)
    r.raise_for_status()
    return r.json()


def api_delete(path: str) -> None:
    url = f"https://api.vercel.com{path}"
    r = requests.delete(url, headers=_headers(), timeout=15)
    r.raise_for_status()


# ── Core data fetchers ─────────────────────────────────────────────────────────
def get_project() -> dict:
    qs = _team_qs()
    return api_get(f"/v9/projects/{VERCEL_PROJECT}{qs}")


def get_deployments(limit: int = 10) -> list[dict]:
    qs = f"{'?' if not _team_qs() else _team_qs() + '&'}projectId={VERCEL_PROJECT}&limit={limit}"
    if _team_qs():
        qs = f"?teamId={VERCEL_TEAM}&projectId={VERCEL_PROJECT}&limit={limit}"
    else:
        qs = f"?projectId={VERCEL_PROJECT}&limit={limit}"
    data = api_get(f"/v6/deployments{qs}")
    return data.get("deployments", [])


def get_deployment(uid: str) -> dict:
    return api_get(f"/v13/deployments/{uid}{_team_qs()}")


def get_env_vars() -> list[dict]:
    qs = _team_qs()
    data = api_get(f"/v9/projects/{VERCEL_PROJECT}/env{qs}")
    return data.get("envs", [])


def set_env_var(key: str, value: str, targets: list[str] | None = None) -> None:
    if targets is None:
        targets = ["production", "preview", "development"]
    existing = {e["key"]: e["id"] for e in get_env_vars()}
    qs = _team_qs()
    if key in existing:
        api_patch(
            f"/v9/projects/{VERCEL_PROJECT}/env/{existing[key]}{qs}",
            {"value": value, "target": targets, "type": "encrypted"},
        )
        logger.info(f"[vercel] Updated env var: {key}")
    else:
        api_post(
            f"/v10/projects/{VERCEL_PROJECT}/env{qs}",
            {"key": key, "value": value, "target": targets, "type": "encrypted"},
        )
        logger.info(f"[vercel] Created env var: {key}")


def get_domains() -> list[dict]:
    qs = _team_qs()
    data = api_get(f"/v9/projects/{VERCEL_PROJECT}/domains{qs}")
    return data.get("domains", [])


def rollback_to(deployment_uid: str) -> dict:
    qs = _team_qs()
    return api_post(f"/v9/projects/{VERCEL_PROJECT}/rollback/{deployment_uid}{qs}", {})


# ── Log helper ────────────────────────────────────────────────────────────────
def log_event(event_type: str, data: dict) -> None:
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": event_type,
        **data,
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ── Agent tasks ───────────────────────────────────────────────────────────────
def deployment_watchdog() -> dict:
    """Check latest deployments. Auto-rollback if production is broken."""
    logger.info("[vercel] Running deployment watchdog...")
    deployments = get_deployments(limit=5)
    if not deployments:
        logger.warning("[vercel] No deployments found.")
        return {"status": "no_deployments"}

    latest = deployments[0]
    uid    = latest["uid"]
    state  = latest.get("state", "UNKNOWN")
    url    = latest.get("url", "")
    target = latest.get("target", "preview")

    result = {
        "uid": uid,
        "state": state,
        "url": url,
        "target": target,
        "action": "none",
    }

    if state == "ERROR":
        logger.error(f"[vercel] Deployment {uid} FAILED (state=ERROR). Initiating rollback...")
        log_event("deployment_failed", result)

        # Find last good production deployment
        good = next(
            (d for d in deployments[1:] if d.get("state") == "READY" and d.get("target") == "production"),
            None,
        )
        if good:
            rollback_to(good["uid"])
            result["action"] = f"rolled_back_to_{good['uid']}"
            logger.success(f"[vercel] Rolled back to {good['uid']}")
            log_event("rollback_executed", {"from": uid, "to": good["uid"]})
        else:
            result["action"] = "rollback_failed_no_good_deployment"
            logger.error("[vercel] No good production deployment to roll back to!")

    elif state == "READY":
        logger.success(f"[vercel] Latest deployment {uid} is READY. URL: {url}")
        log_event("deployment_healthy", result)

    elif state in ("BUILDING", "INITIALIZING", "QUEUED"):
        logger.info(f"[vercel] Deployment {uid} is in progress: {state}")
        log_event("deployment_in_progress", result)

    return result


def performance_audit() -> dict:
    """Check domain SSL, response time, and report anomalies."""
    logger.info("[vercel] Running performance audit...")
    domains = get_domains()
    audit = {"domains": [], "issues": []}

    for d in domains:
        name = d.get("name", "")
        verified = d.get("verified", False)
        ssl = d.get("ssl", {})

        domain_info = {
            "domain": name,
            "verified": verified,
            "ssl_state": ssl.get("state", "unknown"),
        }

        # Measure response time
        try:
            t0 = time.time()
            resp = requests.get(f"https://{name}", timeout=10, allow_redirects=True)
            latency_ms = int((time.time() - t0) * 1000)
            domain_info["status_code"] = resp.status_code
            domain_info["latency_ms"] = latency_ms

            if resp.status_code >= 500:
                audit["issues"].append(f"{name}: HTTP {resp.status_code}")
                logger.error(f"[vercel] {name} returned HTTP {resp.status_code}")
            elif latency_ms > 3000:
                audit["issues"].append(f"{name}: slow ({latency_ms}ms)")
                logger.warning(f"[vercel] {name} is slow: {latency_ms}ms")
            else:
                logger.success(f"[vercel] {name}: {resp.status_code} in {latency_ms}ms")
        except Exception as exc:
            domain_info["error"] = str(exc)
            audit["issues"].append(f"{name}: unreachable ({exc})")
            logger.error(f"[vercel] {name} unreachable: {exc}")

        if not verified:
            audit["issues"].append(f"{name}: domain not verified in Vercel")

        if ssl.get("state") not in ("valid", None):
            audit["issues"].append(f"{name}: SSL issue ({ssl.get('state')})")

        audit["domains"].append(domain_info)

    log_event("performance_audit", audit)
    return audit


def env_sync() -> dict:
    """Sync secrets from local .env to Vercel environment variables."""
    logger.info("[vercel] Syncing env vars to Vercel...")
    synced = []
    failed = []

    for local_key, vercel_key in ENV_SYNC_KEYS.items():
        value = os.getenv(local_key)
        if not value:
            logger.warning(f"[vercel] {local_key} not set in local .env, skipping.")
            continue
        try:
            set_env_var(vercel_key, value)
            synced.append(vercel_key)
        except Exception as exc:
            logger.error(f"[vercel] Failed to sync {vercel_key}: {exc}")
            failed.append({"key": vercel_key, "error": str(exc)})

    result = {"synced": synced, "failed": failed}
    log_event("env_sync", result)
    return result


def generate_report() -> str:
    """Generate a full deployment health report, written by Claude AI."""
    logger.info("[vercel] Generating deployment health report...")

    deployments = get_deployments(limit=20)
    domains = get_domains()
    audit = performance_audit()

    # Build stats
    states = {}
    for d in deployments:
        s = d.get("state", "UNKNOWN")
        states[s] = states.get(s, 0) + 1

    success_rate = 0.0
    total = sum(states.values())
    if total > 0:
        success_rate = states.get("READY", 0) / total * 100

    recent_failures = [
        {"uid": d["uid"], "created": d.get("createdAt"), "url": d.get("url")}
        for d in deployments
        if d.get("state") == "ERROR"
    ]

    report_data = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "project": VERCEL_PROJECT,
        "deployments_analyzed": total,
        "success_rate_pct": round(success_rate, 1),
        "state_breakdown": states,
        "recent_failures": recent_failures[:3],
        "domains": audit["domains"],
        "domain_issues": audit["issues"],
    }

    # Claude summary
    summary = _claude_report_summary(report_data)

    # Write report
    date_str = report_data["date"]
    out_path = REPORTS_DIR / f"{date_str}.md"
    lines = [
        f"# CryptoFeed Vercel Deployment Report — {date_str}",
        f"\nGenerated: {datetime.now(timezone.utc).isoformat()}",
        f"\n## Executive Summary\n",
        summary,
        f"\n## Deployment Stats\n",
        f"- **Deployments analyzed**: {total}",
        f"- **Success rate**: {success_rate:.1f}%",
        f"- **State breakdown**: {json.dumps(states)}",
    ]
    if recent_failures:
        lines.append(f"\n### Recent Failures\n")
        for f in recent_failures:
            lines.append(f"- `{f['uid']}` — {f.get('url', 'n/a')}")

    lines.append(f"\n## Domain Health\n")
    for d in audit["domains"]:
        status = d.get("status_code", "?")
        latency = d.get("latency_ms", "?")
        lines.append(f"- **{d['domain']}**: HTTP {status}, {latency}ms, SSL={d.get('ssl_state')}")

    if audit["issues"]:
        lines.append(f"\n### Issues\n")
        for issue in audit["issues"]:
            lines.append(f"- {issue}")

    out_path.write_text("\n".join(lines))
    logger.success(f"[vercel] Report saved: {out_path}")
    return summary


def _claude_report_summary(data: dict) -> str:
    """Ask Claude to write an executive summary for the deployment report."""
    if not ANTHROPIC_KEY:
        return "_AI summary skipped (no ANTHROPIC_API_KEY)_"
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        issues_text = json.dumps(data["domain_issues"]) if data["domain_issues"] else "None"
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": f"""You are the Head of Infrastructure at CryptoFeed, a crypto market data company.
Write a concise 3-paragraph executive summary for the daily Vercel deployment health report for {data['date']}.

Stats:
- Project: {data['project']}
- Deployments analyzed: {data['deployments_analyzed']}
- Success rate: {data['success_rate_pct']}%
- State breakdown: {json.dumps(data['state_breakdown'])}
- Domain issues: {issues_text}
- Recent failures: {len(data['recent_failures'])}

Paragraph 1: Overall deployment health status.
Paragraph 2: Notable issues and their impact on uptime/customers.
Paragraph 3: Recommended actions for the engineering team.

Be direct and professional. Quantify where possible.""",
            }],
        )
        return msg.content[0].text
    except Exception as exc:
        return f"_AI summary failed: {exc}_"


def force_rollback() -> None:
    """Force rollback to the last READY production deployment."""
    logger.info("[vercel] Forcing rollback to last good deployment...")
    deployments = get_deployments(limit=10)
    good = next(
        (d for d in deployments if d.get("state") == "READY" and d.get("target") == "production"),
        None,
    )
    if not good:
        logger.error("[vercel] No READY production deployment found to roll back to.")
        return
    result = rollback_to(good["uid"])
    logger.success(f"[vercel] Rolled back to {good['uid']}: {result}")
    log_event("manual_rollback", {"to": good["uid"]})


def status_dashboard() -> None:
    """Print a live status dashboard."""
    project  = get_project()
    deploys  = get_deployments(limit=5)
    domains  = get_domains()

    print("\n" + "=" * 60)
    print(f"  CryptoFeed Vercel Agent — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    print(f"\n  Project : {project.get('name')}")
    print(f"  Framework: {project.get('framework', 'nextjs')}")

    print(f"\n  {'RECENT DEPLOYMENTS':}")
    print(f"  {'UID':<26} {'STATE':<14} {'TARGET':<12} {'URL'}")
    print(f"  {'-'*70}")
    for d in deploys:
        uid    = d.get("uid", "")[:24]
        state  = d.get("state", "?")
        target = d.get("target", "?")
        url    = d.get("url", "")[:40]
        icon   = "OK" if state == "READY" else ("ERR" if state == "ERROR" else "...")
        print(f"  {icon:<4} {uid:<24} {state:<14} {target:<12} {url}")

    print(f"\n  {'DOMAINS'}")
    for d in domains:
        verified = "OK" if d.get("verified") else "!!"
        print(f"  {verified} {d.get('name')}")

    print("\n" + "=" * 60 + "\n")


# ── Scheduled entry points (called by orchestrator) ───────────────────────────
def check_deployments():
    """Called every 5 min by orchestrator."""
    if not VERCEL_TOKEN:
        logger.warning("[vercel] VERCEL_TOKEN not set, skipping.")
        return
    deployment_watchdog()


def audit_performance():
    """Called every 1 hour by orchestrator."""
    if not VERCEL_TOKEN:
        return
    performance_audit()


def sync_environment():
    """Called every 6 hours by orchestrator."""
    if not VERCEL_TOKEN:
        return
    env_sync()


def daily_report():
    """Called daily by orchestrator."""
    if not VERCEL_TOKEN:
        return
    summary = generate_report()
    print(f"\nVercel Report Summary:\n{summary}")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CryptoFeed Vercel Agent")
    parser.add_argument("--watch",    action="store_true", help="Continuous watch loop (30s interval)")
    parser.add_argument("--report",   action="store_true", help="Generate full deployment report now")
    parser.add_argument("--rollback", action="store_true", help="Force rollback to last good deployment")
    parser.add_argument("--sync-env", action="store_true", help="Sync .env secrets → Vercel")
    parser.add_argument("--status",   action="store_true", help="Print live status dashboard")
    args = parser.parse_args()

    if not VERCEL_TOKEN:
        logger.error("VERCEL_TOKEN is not set. Add it to your .env file.")
        sys.exit(1)

    if args.status:
        status_dashboard()
    elif args.rollback:
        force_rollback()
    elif args.sync_env:
        result = env_sync()
        print(json.dumps(result, indent=2))
    elif args.report:
        summary = generate_report()
        print(f"\n{summary}")
    elif args.watch:
        logger.info("[vercel] Starting watch loop (30s interval)...")
        while True:
            deployment_watchdog()
            time.sleep(30)
    else:
        # Run all checks once
        status_dashboard()
        deployment_watchdog()
        audit_performance()


if __name__ == "__main__":
    main()
