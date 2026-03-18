"""Available symbols endpoint."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, Depends

from api.auth import optional_auth, APIKeyInfo
from api.models import SymbolInfo

router = APIRouter(tags=["symbols"])


@router.get("/symbols", response_model=list[SymbolInfo])
def list_symbols(key: APIKeyInfo = Depends(optional_auth)) -> list[SymbolInfo]:
    """List all available exchange/symbol combinations with date coverage."""
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    results = []

    for dtype_dir in sorted(data_dir.iterdir()) if data_dir.exists() else []:
        data_type = dtype_dir.name
        if not dtype_dir.is_dir():
            continue
        for exchange_dir in sorted(dtype_dir.iterdir()):
            if not exchange_dir.is_dir():
                continue
            exchange = exchange_dir.name
            for symbol_dir in sorted(exchange_dir.iterdir()):
                if not symbol_dir.is_dir():
                    continue
                symbol = symbol_dir.name
                dates = sorted(d.name for d in symbol_dir.iterdir() if d.is_dir())
                key_str = f"{exchange}/{symbol}"

                # Find or update existing entry
                existing = next((r for r in results if r.exchange == exchange and r.symbol == symbol), None)
                if existing is None:
                    results.append(SymbolInfo(
                        exchange=exchange,
                        symbol=symbol,
                        data_types=[data_type],
                        earliest_date=dates[0] if dates else None,
                        latest_date=dates[-1] if dates else None,
                    ))
                else:
                    if data_type not in existing.data_types:
                        existing.data_types.append(data_type)

    return sorted(results, key=lambda x: (x.exchange, x.symbol))
