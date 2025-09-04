# src/reports/loaders.py
from __future__ import annotations

from typing import Dict, Tuple, List
import numpy as np
import pandas as pd
from sqlalchemy import text
from src.core.db import get_engine


# ---------- утилы ----------
def _zscore(arr: np.ndarray) -> np.ndarray:
    mu = float(np.nanmean(arr))
    sd = float(np.nanstd(arr))
    if sd == 0 or np.isnan(sd):
        return np.zeros_like(arr, dtype=float)
    return (arr - mu) / sd


# ---------- что есть в БД ----------
def get_macro_codes_from_db() -> List[str]:
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(
            text("""
                select s.code
                from core.series s
                where s.asset_class='macro'
                  and exists (select 1 from core.observation o where o.series_id=s.series_id)
                order by s.code
            """)
        ).fetchall()
    return [r.code for r in rows]


# ---------- SPX payload для Timeseries ----------
def load_spx_payload() -> Tuple[Dict[str, object], str]:
    """
    Вернёт:
      payload = {
        "ts": [... iso ...],
        "log": [...],
        "log_z": [...],
        "ohlc": { "open": [...], "high": [...], "low": [...], "close": [...] }
      }, spx_code
    """
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql(
            text("""
                select p.ts,
                       p.open, p.high, p.low, p.close, p.adj_close,
                       s.code as asset_code
                from core.price p
                join core.series s using(series_id)
                where s.code in ('^GSPC','SPY')
                order by p.ts
            """),
            c,
        )

    if df.empty:
        raise RuntimeError("В БД нет ни ^GSPC, ни SPY")

    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]

    df = (
        df[df["asset_code"] == preferred]
        .dropna(subset=["close"])  # минимум — есть close
        .sort_values("ts")
        .reset_index(drop=True)
    )

    # для close используем coalesce(adj_close, close) (как раньше)
    co_close = df["adj_close"].fillna(df["close"]).astype(float).to_numpy()
    log_px = np.log(co_close).astype(float)
    log_px_z = _zscore(log_px)

    payload = {
        "ts": [pd.Timestamp(t).isoformat() for t in df["ts"]],
        "log": [float(x) for x in log_px],
        "log_z": [float(x) for x in log_px_z],
        "ohlc": {
            "open": [None if pd.isna(x) else float(x) for x in df["open"].astype(float)],
            "high": [None if pd.isna(x) else float(x) for x in df["high"].astype(float)],
            "low":  [None if pd.isna(x) else float(x) for x in df["low"].astype(float)],
            # для свечей берём не скорректированный close (классика свечей)
            "close":[None if pd.isna(x) else float(x) for x in df["close"].astype(float)],
        }
    }
    return payload, preferred


# ---------- макро payload для Timeseries ----------
def load_macro_series_payload() -> Tuple[Dict[str, dict], str]:
    """
    { CODE: { ts:[iso], raw:[float], z:[float] }, ... }, default_code
    """
    eng = get_engine()
    payload: Dict[str, dict] = {}
    default_code: str | None = None

    for code in get_macro_codes_from_db():
        with eng.connect() as c:
            df = pd.read_sql(
                text("""
                    select o.ts, o.value
                    from core.observation o
                    join core.series s using(series_id)
                    where s.code=:code
                    order by o.ts
                """),
                c,
                params={"code": code},
            )

        if df.empty:
            continue

        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.dropna(subset=["value"]).sort_values("ts")

        v = df["value"].astype(float).to_numpy()
        z = _zscore(v)

        payload[code] = {
            "ts": [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "raw": [float(x) if x is not None else None for x in v],
            "z":   [float(x) if x is not None else None for x in z],
        }
        if default_code is None or code == "ICSA":
            default_code = code

    if not payload:
        raise RuntimeError("Нет макро-рядов (asset_class='macro')")

    if default_code is None:
        default_code = sorted(payload.keys())[0]

    return payload, default_code
