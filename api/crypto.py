"""Application-level encryption for secrets at rest.

Uses Fernet (AES-128-CBC + HMAC-SHA256) from the cryptography package.
api_secret values are ALWAYS stored encrypted. Plaintext never touches disk.

Key management:
  Generate:  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  Set:       SECRET_ENCRYPTION_KEY=<base64url-key> in .env
  Rotate:    python api/crypto.py rotate  (re-encrypts all rows in users.db)

Production behaviour:
  - If SECRET_ENCRYPTION_KEY is unset → hard exit (sys.exit(1))
  - If key is invalid format        → hard exit (sys.exit(1))

Development behaviour (ENVIRONMENT != "production"):
  - If SECRET_ENCRYPTION_KEY is unset → uses insecure dev key with loud warning
  - Key is deterministic and well-known — NEVER deploy without a real key
"""
from __future__ import annotations

import os
import sys

_ENV = os.getenv("ENVIRONMENT", "development").lower()

# ── Dev fallback key (all-zero bytes — INSECURE, clearly labelled) ─────────────
_DEV_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

# Resolve key via KMS / Vault / env var (api.kms handles the fallback chain)
try:
    from api.kms import get_encryption_key as _get_key
    _RAW_KEY = _get_key()
except RuntimeError as _kms_err:
    # RuntimeError is raised by kms.py only in production with no key configured
    print(f"[FATAL] {_kms_err}", file=sys.stderr)
    sys.exit(1)
except Exception as _kms_err:
    # Unexpected failure — safe to re-read from env as last resort
    _RAW_KEY = os.getenv("SECRET_ENCRYPTION_KEY", "").strip()
    if not _RAW_KEY:
        if _ENV == "production":
            print(
                f"[FATAL] Encryption key unavailable ({_kms_err}). "
                "Set SECRET_ENCRYPTION_KEY or configure AWS_KMS_KEY_ID / VAULT_ADDR.",
                file=sys.stderr,
            )
            sys.exit(1)
        _RAW_KEY = _DEV_KEY
        print("[WARNING] crypto: using insecure dev key — set SECRET_ENCRYPTION_KEY", file=sys.stderr)

try:
    from cryptography.fernet import Fernet, InvalidToken as _InvalidToken
    _fernet = Fernet(_RAW_KEY.encode() if isinstance(_RAW_KEY, str) else _RAW_KEY)
except Exception as exc:
    print(f"[FATAL] Failed to initialise encryption key: {exc}", file=sys.stderr)
    sys.exit(1)


# ── Public API ──────────────────────────────────────────────────────────────────

def encrypt_secret(plaintext: str) -> str:
    """Encrypt a plaintext secret. Returns a Fernet token (base64url string)."""
    if not plaintext:
        return ""
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    """Decrypt a Fernet token. Raises ValueError if invalid or tampered."""
    if not ciphertext:
        return ""
    try:
        return _fernet.decrypt(ciphertext.encode()).decode()
    except _InvalidToken as exc:
        raise ValueError(
            "Secret decryption failed — key mismatch or tampered ciphertext"
        ) from exc


def is_encrypted(value: str) -> bool:
    """Return True if value looks like a Fernet token (starts with 'gAAAAA').

    Fernet tokens always start with the version byte 0x80, which base64url-encodes
    to 'gAAAAA'. Used to detect unencrypted legacy rows during migration.
    """
    return bool(value) and value.startswith("gAAAAA")


# ── Key rotation CLI ────────────────────────────────────────────────────────────

def rotate_secrets(db_path: str, new_key: str) -> int:
    """
    Re-encrypt all api_secret values in users.db with a new Fernet key.

    Steps:
        1. Decrypt every api_secret with the CURRENT key (from env).
        2. Re-encrypt with new_key.
        3. Write back atomically.

    Returns number of rows rotated.

    Usage:
        SECRET_ENCRYPTION_KEY=<old_key> python api/crypto.py rotate --new-key <new_key>
    """
    import sqlite3
    from cryptography.fernet import Fernet
    new_fernet = Fernet(new_key.encode() if isinstance(new_key, str) else new_key)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT api_key_hash, api_secret FROM users WHERE api_secret IS NOT NULL AND api_secret != ''"
    ).fetchall()

    rotated = 0
    for row in rows:
        try:
            plaintext = decrypt_secret(row["api_secret"])
            new_ciphertext = new_fernet.encrypt(plaintext.encode()).decode()
            conn.execute(
                "UPDATE users SET api_secret = ? WHERE api_key_hash = ?",
                (new_ciphertext, row["api_key_hash"])
            )
            rotated += 1
        except Exception as exc:
            print(f"  [WARN] Could not rotate key {row['api_key_hash'][:16]}: {exc}", file=sys.stderr)

    conn.commit()
    conn.close()
    return rotated


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Secret key management")
    sub = parser.add_subparsers(dest="cmd")

    rot = sub.add_parser("rotate", help="Re-encrypt all secrets with a new key")
    rot.add_argument("--new-key", required=True, help="New Fernet key (base64url, 44 chars)")
    rot.add_argument("--db", default=os.path.join(os.getenv("DATA_DIR", "./data"), "users.db"))

    args = parser.parse_args()
    if args.cmd == "rotate":
        n = rotate_secrets(args.db, args.new_key)
        print(f"Rotated {n} secret(s). Update SECRET_ENCRYPTION_KEY to the new value.")
    else:
        parser.print_help()
