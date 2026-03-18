"""API key management CLI.

Usage:
    python api/keys.py generate --plan pro --owner user@example.com
    python api/keys.py list
    python api/keys.py revoke <key_hash>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sys
from pathlib import Path

KEYS_FILE = Path(__file__).parent / "keys.json"


def _load() -> dict:
    if not KEYS_FILE.exists():
        return {}
    return json.loads(KEYS_FILE.read_text())


def _save(data: dict) -> None:
    KEYS_FILE.write_text(json.dumps(data, indent=2))


def generate(plan: str, owner: str) -> str:
    key = f"cf_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    data = _load()
    data[key_hash] = {"plan": plan, "owner": owner}
    _save(data)
    print(f"\nAPI Key generated:")
    print(f"  Key   : {key}")
    print(f"  Hash  : {key_hash[:16]}...")
    print(f"  Plan  : {plan}")
    print(f"  Owner : {owner}")
    print(f"\nShare the key with the customer. The hash is stored (not the key itself).")
    return key


def list_keys() -> None:
    data = _load()
    if not data:
        print("No keys.")
        return
    print(f"\n{'Hash (first 16)':<20} {'Plan':<12} {'Owner'}")
    print("-" * 60)
    for h, v in data.items():
        print(f"  {h[:16]:<18} {v['plan']:<12} {v['owner']}")


def revoke(key_hash_prefix: str) -> None:
    data = _load()
    matches = [h for h in data if h.startswith(key_hash_prefix)]
    if not matches:
        print(f"No key found with hash prefix: {key_hash_prefix}")
        return
    for h in matches:
        del data[h]
        print(f"Revoked: {h[:16]}...")
    _save(data)


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
