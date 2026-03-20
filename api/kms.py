"""Encryption key provider — AWS KMS, HashiCorp Vault, or env var fallback.

Retrieval order (first success wins):
  1. AWS KMS        — if AWS_KMS_KEY_ID + AWS_KMS_CIPHERTEXT are set
  2. HashiCorp Vault — if VAULT_ADDR + VAULT_TOKEN + VAULT_SECRET_PATH are set
  3. Env var         — SECRET_ENCRYPTION_KEY
  4. Dev key         — only allowed outside production (logs a loud warning)

The resolved key is cached in-process after the first call.
Call invalidate_cache() after a key rotation.

Setup guides
────────────
AWS KMS:
  1. Create a KMS symmetric key (console or CLI).
  2. Encrypt your Fernet key: aws kms encrypt --key-id <KEY_ID> --plaintext fileb://<(echo -n "$FERNET_KEY") --output text --query CiphertextBlob
  3. Set AWS_KMS_KEY_ID=<arn-or-alias> and AWS_KMS_CIPHERTEXT=<base64-blob>.
  4. Grant the ECS/EC2 role kms:Decrypt on that key.

HashiCorp Vault (KV v2):
  1. vault kv put secret/cryptofeed encryption_key="<FERNET_KEY>"
  2. Set VAULT_ADDR, VAULT_TOKEN (or use VAULT_ROLE_ID + VAULT_SECRET_ID for AppRole),
     VAULT_SECRET_PATH=secret/data/cryptofeed, VAULT_SECRET_KEY=encryption_key.
"""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from typing import Optional

_DEV_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


@lru_cache(maxsize=1)
def get_encryption_key() -> str:
    """Return the Fernet encryption key as a base64url string.

    Raises RuntimeError in production if no key source is available.
    """
    env = os.getenv("ENVIRONMENT", "development").lower()

    key = _try_aws_kms()
    if key:
        _log(f"[kms] Encryption key loaded from AWS KMS")
        return key

    key = _try_vault()
    if key:
        _log("[kms] Encryption key loaded from HashiCorp Vault")
        return key

    key = os.getenv("SECRET_ENCRYPTION_KEY", "").strip()
    if key:
        return key

    if env == "production":
        raise RuntimeError(
            "[FATAL] No encryption key available. "
            "Set SECRET_ENCRYPTION_KEY, AWS_KMS_KEY_ID+AWS_KMS_CIPHERTEXT, "
            "or VAULT_ADDR+VAULT_TOKEN+VAULT_SECRET_PATH."
        )

    print(
        "[WARNING] kms: No encryption key configured — using insecure dev key. "
        "Set SECRET_ENCRYPTION_KEY before any production deployment.",
        file=sys.stderr,
    )
    return _DEV_KEY


def invalidate_cache() -> None:
    """Clear the cached key — call after a key rotation."""
    get_encryption_key.cache_clear()


# ── Providers ──────────────────────────────────────────────────────────────────

def _try_aws_kms() -> Optional[str]:
    """Decrypt the Fernet key stored as a KMS ciphertext blob.

    Required env vars:
        AWS_KMS_KEY_ID      — KMS key ARN or alias
        AWS_KMS_CIPHERTEXT  — base64-encoded ciphertext produced by kms:Encrypt
        AWS_REGION          — AWS region (default: us-east-1)
    """
    key_id    = os.getenv("AWS_KMS_KEY_ID", "")
    cipher_b64 = os.getenv("AWS_KMS_CIPHERTEXT", "")
    if not key_id or not cipher_b64:
        return None
    try:
        import base64
        import boto3
        kms = boto3.client("kms", region_name=os.getenv("AWS_REGION", "us-east-1"))
        ciphertext = base64.b64decode(cipher_b64)
        response = kms.decrypt(CiphertextBlob=ciphertext, KeyId=key_id)
        plaintext = response["Plaintext"]
        return plaintext.decode() if isinstance(plaintext, bytes) else str(plaintext)
    except ImportError:
        return None
    except Exception as exc:
        print(f"[WARNING] kms: AWS KMS decryption failed: {exc}", file=sys.stderr)
        return None


def _try_vault() -> Optional[str]:
    """Read a Fernet key from HashiCorp Vault KV store.

    Required env vars:
        VAULT_ADDR         — e.g. https://vault.example.com
        VAULT_TOKEN        — Vault auth token
        VAULT_SECRET_PATH  — KV path, e.g. secret/data/cryptofeed

    Optional:
        VAULT_SECRET_KEY   — field name within the secret (default: encryption_key)
    """
    addr  = os.getenv("VAULT_ADDR", "")
    token = os.getenv("VAULT_TOKEN", "")
    path  = os.getenv("VAULT_SECRET_PATH", "")
    if not addr or not token or not path:
        return None
    try:
        import json
        import urllib.request

        secret_key = os.getenv("VAULT_SECRET_KEY", "encryption_key")
        url = f"{addr.rstrip('/')}/v1/{path.lstrip('/')}"
        req = urllib.request.Request(url, headers={"X-Vault-Token": token})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        # KV v2 wraps data under data.data; KV v1 uses data directly
        secret_data = data.get("data", {})
        if "data" in secret_data:
            secret_data = secret_data["data"]
        return secret_data.get(secret_key) or None
    except Exception as exc:
        print(f"[WARNING] kms: Vault retrieval failed: {exc}", file=sys.stderr)
        return None


def _log(msg: str) -> None:
    try:
        from loguru import logger
        logger.info(msg)
    except ImportError:
        print(msg, file=sys.stderr)
