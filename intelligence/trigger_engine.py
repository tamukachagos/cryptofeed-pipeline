"""
Layer 4 — Condition-Based Trigger Engine.

Evaluates a set of user-defined conditions against live intelligence snapshots
and fires TriggerEvent objects when conditions are met.

Designed for:
    - Systematic strategy entry/exit signals
    - Risk alerts (regime change, cascade detected)
    - Webhook delivery to downstream systems

Trigger definition:
    {
        "id":         "my_trigger_001",
        "symbol":     "BTCUSDT",
        "conditions": [
            {"field": "regime",           "op": "eq",  "value": "trending_bull"},
            {"field": "edge_bps",         "op": "gte", "value": 5.0},
            {"field": "realised_vol",     "op": "lt",  "value": 0.80},
        ],
        "require_all": true,            # AND vs OR
        "cooldown_s":  60,              # minimum seconds between repeated fires
    }

Supported fields from intelligence layer:
    From RegimeSnapshot:   regime, confidence, momentum, realised_vol, spread_bps,
                           obi, flow_imbalance, structural_break
    From AnomalyEvent:     anomaly_type, severity (as anomaly_severity)
    From ArbSignal:        edge_bps, arb_type, confidence (as arb_confidence)

Supported ops: eq, ne, gt, gte, lt, lte, in, contains
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from intelligence.regime_detector import RegimeSnapshot
from intelligence.anomaly_detector import AnomalyEvent
from intelligence.arb_detector import ArbSignal

_DB_PATH = Path(os.getenv("DATA_DIR", "./data")) / "users.db"


@dataclass
class TriggerEvent:
    trigger_id:    str
    symbol:        str
    timestamp_ns:  int
    fired_on:      str             # "regime" | "anomaly" | "arb"
    matched_value: dict            # the snapshot fields that matched
    trigger_def:   dict

    @property
    def timestamp_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            "trigger_id":    self.trigger_id,
            "symbol":        self.symbol,
            "timestamp_ns":  self.timestamp_ns,
            "timestamp":     self.timestamp_dt.isoformat(),
            "fired_on":      self.fired_on,
            "matched_value": self.matched_value,
        }


_OPS = {
    "eq":       lambda a, b: a == b,
    "ne":       lambda a, b: a != b,
    "gt":       lambda a, b: float(a) > float(b),
    "gte":      lambda a, b: float(a) >= float(b),
    "lt":       lambda a, b: float(a) < float(b),
    "lte":      lambda a, b: float(a) <= float(b),
    "in":       lambda a, b: a in b,
    "contains": lambda a, b: str(b).lower() in str(a).lower(),
}


class TriggerEngine:
    """
    Evaluates registered triggers against intelligence layer outputs.
    Triggers are persisted to SQLite (users.db) and reloaded on startup.

    Ownership model:
        Each trigger has an `_owner` field (key_hash[:16]) set by the API router.
        The router enforces ownership on list/delete; TriggerEngine stores it as-is.

    SQLite schema (in users.db):
        CREATE TABLE triggers (
            id TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '*',
            conditions TEXT NOT NULL,   -- JSON array
            require_all INTEGER NOT NULL DEFAULT 1,
            cooldown_s REAL NOT NULL DEFAULT 60.0,
            created_at TEXT NOT NULL,
            extra TEXT DEFAULT '{}'     -- JSON blob for forward-compat fields
        )
    """

    def __init__(self) -> None:
        self._triggers: list[dict]           = []
        self._last_fired: dict[str, float]   = {}  # trigger_id → monotonic time
        self._ensure_schema()
        self._load_from_db()

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        try:
            conn = self._get_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS triggers (
                    id         TEXT PRIMARY KEY,
                    owner      TEXT NOT NULL DEFAULT '',
                    symbol     TEXT NOT NULL DEFAULT '*',
                    conditions TEXT NOT NULL DEFAULT '[]',
                    require_all INTEGER NOT NULL DEFAULT 1,
                    cooldown_s REAL NOT NULL DEFAULT 60.0,
                    created_at TEXT NOT NULL DEFAULT '',
                    extra      TEXT DEFAULT '{}'
                )
            """)
            conn.commit()
            conn.close()
        except Exception:
            pass  # DB not available — operate in-memory only

    def _load_from_db(self) -> None:
        """Load all triggers from DB on startup (idempotent)."""
        try:
            conn = self._get_conn()
            rows = conn.execute("SELECT * FROM triggers").fetchall()
            conn.close()
            loaded: list[dict] = []
            for row in rows:
                d = dict(row)
                # Reconstruct the trigger_def dict
                extra = json.loads(d.get("extra") or "{}")
                tdef = {
                    "id":          d["id"],
                    "_owner":      d["owner"],
                    "symbol":      d["symbol"],
                    "conditions":  json.loads(d["conditions"]),
                    "require_all": bool(d["require_all"]),
                    "cooldown_s":  d["cooldown_s"],
                    **extra,
                }
                loaded.append(tdef)
            self._triggers = loaded
        except Exception:
            pass  # DB unavailable — start with empty in-memory list

    def _save_trigger(self, tdef: dict) -> None:
        """Upsert one trigger to SQLite."""
        try:
            known_keys = {"id", "_owner", "symbol", "conditions", "require_all", "cooldown_s"}
            extra = {k: v for k, v in tdef.items() if k not in known_keys}
            conn = self._get_conn()
            conn.execute(
                """INSERT OR REPLACE INTO triggers
                   (id, owner, symbol, conditions, require_all, cooldown_s, created_at, extra)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    tdef["id"],
                    tdef.get("_owner", ""),
                    tdef.get("symbol", "*"),
                    json.dumps(tdef.get("conditions", [])),
                    int(bool(tdef.get("require_all", True))),
                    float(tdef.get("cooldown_s", 60.0)),
                    datetime.now(timezone.utc).isoformat(),
                    json.dumps(extra),
                )
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def _delete_trigger(self, trigger_id: str) -> None:
        """Remove one trigger from SQLite."""
        try:
            conn = self._get_conn()
            conn.execute("DELETE FROM triggers WHERE id = ?", (trigger_id,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    # ── Public API ────────────────────────────────────────────────────────────

    def register(self, trigger_def: dict) -> None:
        """Register (upsert) a trigger. Persists to DB."""
        tid = trigger_def.get("id")
        if not tid:
            raise ValueError("Trigger must have an 'id' field")
        self._triggers = [t for t in self._triggers if t.get("id") != tid]
        self._triggers.append(trigger_def)
        self._save_trigger(trigger_def)

    def unregister(self, trigger_id: str) -> None:
        """Remove a trigger. Persists deletion to DB."""
        self._triggers = [t for t in self._triggers if t.get("id") != trigger_id]
        self._delete_trigger(trigger_id)

    def list_triggers(self) -> list[dict]:
        return list(self._triggers)

    # ── Evaluation entry points ───────────────────────────────────────────────

    def evaluate_regime(self, snap: RegimeSnapshot) -> list[TriggerEvent]:
        ctx = {
            "regime":           snap.regime.value,
            "confidence":       snap.confidence,
            "momentum":         snap.momentum,
            "realised_vol":     snap.realised_vol,
            "spread_bps":       snap.spread_bps or 0.0,
            "obi":              snap.obi or 0.0,
            "flow_imbalance":   snap.flow_imbalance,
            "structural_break": snap.structural_break,
        }
        return self._check_all("regime", snap.symbol, snap.timestamp_ns, ctx)

    def evaluate_anomaly(self, event: AnomalyEvent) -> list[TriggerEvent]:
        ctx = {
            "anomaly_type":     event.anomaly_type.value,
            "anomaly_severity": event.severity,
            "confidence":       event.confidence,
        }
        return self._check_all("anomaly", event.symbol, event.timestamp_ns, ctx)

    def evaluate_arb(self, signal: ArbSignal) -> list[TriggerEvent]:
        ctx = {
            "arb_type":        signal.arb_type.value,
            "edge_bps":        signal.edge_bps,
            "arb_confidence":  signal.confidence,
            "exchange_a":      signal.exchange_a,
            "exchange_b":      signal.exchange_b,
        }
        return self._check_all("arb", signal.symbol, signal.timestamp_ns, ctx)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _check_all(
        self,
        fired_on: str,
        symbol: str,
        timestamp_ns: int,
        ctx: dict[str, Any],
    ) -> list[TriggerEvent]:
        fired: list[TriggerEvent] = []

        for tdef in self._triggers:
            # Symbol filter
            trig_symbol = tdef.get("symbol", "*")
            if trig_symbol != "*" and trig_symbol != symbol:
                continue

            # Cooldown
            tid = tdef["id"]
            cooldown = float(tdef.get("cooldown_s", 0))
            if cooldown > 0:
                last = self._last_fired.get(tid, 0.0)
                if time.monotonic() - last < cooldown:
                    continue

            # Evaluate conditions
            conditions  = tdef.get("conditions", [])
            require_all = tdef.get("require_all", True)
            results     = [self._eval_condition(c, ctx) for c in conditions]

            if not results:
                continue

            matched = all(results) if require_all else any(results)
            if not matched:
                continue

            self._last_fired[tid] = time.monotonic()
            fired.append(TriggerEvent(
                trigger_id=tid,
                symbol=symbol,
                timestamp_ns=timestamp_ns,
                fired_on=fired_on,
                matched_value={k: v for k, v in ctx.items()},
                trigger_def=tdef,
            ))

        return fired

    def _eval_condition(self, condition: dict, ctx: dict) -> bool:
        field_name = condition.get("field", "")
        op_name    = condition.get("op", "eq")
        value      = condition.get("value")

        if field_name not in ctx:
            return False

        op_fn = _OPS.get(op_name)
        if op_fn is None:
            return False

        try:
            return bool(op_fn(ctx[field_name], value))
        except (TypeError, ValueError):
            return False
