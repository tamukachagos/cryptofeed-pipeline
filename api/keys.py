"""API key management CLI.

Usage:
    python api/keys.py generate --plan pro --owner user@example.com
    python api/keys.py list
    python api/keys.py revoke <key_hash_prefix>
"""
from __future__ import annotations

import argparse
import hashlib
import os
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.crypto import encrypt_secret
from api.db import get_db


def _get_db():
    return get_db()


def generate(plan: str, owner: str) -> tuple[str, str]:
    api_key    = f"cda_{secrets.token_urlsafe(32)}"
    api_secret = secrets.token_urlsafe(48)
    key_hash   = hashlib.sha256(api_key.encode()).hexdigest()
    key_prefix = api_key[:12] + "..."
    now        = datetime.now(timezone.utc).isoformat()

    encrypted_secret = encrypt_secret(api_secret)

    conn = _get_db()
    conn.execute(
        """INSERT OR REPLACE INTO users
           (email, plan, api_key_hash, api_key_prefix, api_secret, created_at,
            requests_today, requests_this_month, daily_usage)
           VALUES (?, ?, ?, ?, ?, ?, 0, 0, '0,0,0,0,0,0,0')""",
        (owner, plan, key_hash, key_prefix, encrypted_secret, now),
    )
    conn.commit()
    conn.close()

    print(f"\nAPI credentials generated:")
    print(f"  api_key    : {api_key}")
    print(f"  api_secret : {api_secret}")
    print(f"  Hash       : {key_hash[:16]}...")
    print(f"  Plan       : {plan}")
    print(f"  Owner      : {owner}")
    print(f"\nShare BOTH values with the customer (shown only once).")
    print(f"  api_key    → X-API-Key header (public identifier)")
    print(f"  api_secret → HMAC-SHA256 signing key (keep private)")
    return api_key, api_secret


def list_keys() -> None:
    conn = _get_db()
    rows = conn.execute(
        "SELECT email, plan, api_key_hash, api_key_prefix, created_at FROM users WHERE api_key_hash IS NOT NULL"
    ).fetchall()
    conn.close()

    if not rows:
        print("No keys.")
        return

    print(f"\n{'Hash (first 16)':<20} {'Plan':<12} {'Owner'}")
    print("-" * 60)
    for r in rows:
        h = r["api_key_hash"] or ""
        print(f"  {h[:16]:<18} {r['plan']:<12} {r['email']}")


def revoke(key_hash_prefix: str) -> None:
    conn = _get_db()
    rows = conn.execute(
        "SELECT email, api_key_hash FROM users WHERE api_key_hash IS NOT NULL"
    ).fetchall()
    matches = [r for r in rows if r["api_key_hash"].startswith(key_hash_prefix)]
    if not matches:
        print(f"No key found with hash prefix: {key_hash_prefix}")
        conn.close()
        return

    confirm = input(f"Revoke {len(matches)} key(s)? [y/N] ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        conn.close()
        return

    for r in matches:
        conn.execute(
            "UPDATE users SET api_key_hash = NULL, api_key_prefix = NULL, api_secret = NULL WHERE email = ?",
            (r["email"],)
        )
        print(f"Revoked key for: {r['email']} ({r['api_key_hash'][:16]}...)")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API key management")
    sub = parser.add_subparsers(dest="cmd")

    gen = sub.add_parser("generate")
    gen.add_argument("--plan", choices=["free", "pro", "enterprise"], default="free")
    gen.add_argument("--owner", required=True)

    sub.add_parser("list")

    rev = sub.add_parser("revoke")
    rev.add_argument("key_hash", help="Full or partial key hash")

    args = parser.parse_args()
    if args.cmd == "generate":
        generate(args.plan, args.owner)
    elif args.cmd == "list":
        list_keys()
    elif args.cmd == "revoke":
        revoke(args.key_hash)
    else:
        parser.print_help()
