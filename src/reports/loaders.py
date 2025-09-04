# src/reports/loaders.py
from __future__ import annotations

from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.core.db import get_engine


# ---------- вспомогательные ----------
def _to_monthly_last(s: pd.Series) -> pd.Series:
    return s.sort_index().resample("ME").last()

def _zscore(x: pd.Series) -> pd.Series:
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if pd.isna(sd) or sd == 0:
        return pd.Series(np.zeros(len(x)), index=x.index)
    return (x - mu) / sd


# ---------- цены ----------
def load_spx_from_db(start: str = "2016-01-01") -> Tuple[pd.DataFrame, str]:
    """
    Возвращает df с колонками [ts, px] и код инструмента (предпочтительно ^GSPC, иначе SPY).
    """
    eng = get_engine()
    with eng.connect() as c:
        q = text("""
            select p.ts, coalesce(p.adj_close, p.close) as px, s.code as asset_code
            from core.price p
            join core.series s using(series_id)
            where s.code in ('^GSPC','SPY') and p.ts >= :start
            order by p.ts
        """)
        df = pd.read_sql(q, c, params={"start": pd.Timestamp(start, tz="UTC")})

    if df.empty:
        raise RuntimeError("В БД нет ни ^GSPC, ни SPY (после фильтра по дате).")

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    df = df[df["asset_code"] == preferred].dropna(subset=["px"]).sort_values("ts")
    return df[["ts", "px"]], preferred


def load_price_payload(start: str = "2016-01-01") -> Tuple[Dict[str, dict], str]:
    """
    Готовит payload цен для фронта по всем сериям core.series с asset_class in ('equity','fx','metal'),
    у которых есть данные в core.price, начиная с 'start'.
    Формат на выходе:
      { CODE: { ts: [...iso], open: [...], high: [...], low: [...], close: [...] }, ... }, default_code
    default_code — предпочитаем ^GSPC, иначе первый по алфавиту.
    """
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(text("""
            select s.series_id, s.code
            from core.series s
            where s.asset_class in ('equity','fx','metal')
              and exists (
                select 1 from core.price p
                where p.series_id = s.series_id and p.ts >= :start
              )
            order by s.code
        """), {"start": pd.Timestamp(start, tz="UTC")}).fetchall()

    payload: Dict[str, dict] = {}
    default_code = None

    eng = get_engine()
    for r in rows:
        sid, code = r.series_id, r.code
        with eng.connect() as c:
            df = pd.read_sql(text("""
                select ts, open, high, low, close, coalesce(adj_close, close) as adj_close
                from core.price
                where series_id = :sid and ts >= :start
                order by ts
            """), c, params={"sid": sid, "start": pd.Timestamp(start, tz="UTC")})

        if df.empty:
            continue

        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        for col in ["open", "high", "low", "close", "adj_close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"]).sort_values("ts")

        payload[code] = {
            "ts":   [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "open": [float(x) if x is not None else None for x in df["open"]],
            "high": [float(x) if x is not None else None for x in df["high"]],
            "low":  [float(x) if x is not None else None for x in df["low"]],
            "close":[float(x) if x is not None else None for x in df["close"]],
        }
        if default_code is None:
            default_code = code

    if "^GSPC" in payload:
        default_code = "^GSPC"

    if not payload:
        raise RuntimeError("Нет ценовых рядов (equity/fx/metal) после 2016-01-01.")

    return payload, default_code


# ---------- макро ----------
def get_macro_codes_from_db() -> List[str]:
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(text("""
            select s.code
            from core.series s
            where s.asset_class='macro'
              and exists (select 1 from core.observation o where o.series_id=s.series_id)
            order by s.code
        """)).fetchall()
    return [r.code for r in rows]


def load_macro_series_payload() -> Tuple[Dict[str, dict], str]:
    """
    Возвращает:
      payload = { CODE: { ts:[iso], raw:[float], z:[float] }, ... }
      default_code
    """
    eng = get_engine()
    payload: Dict[str, dict] = {}
    default_code: str | None = None

    for code in get_macro_codes_from_db():
        with eng.connect() as c:
            df = pd.read_sql(text("""
                select o.ts, o.value
                from core.observation o
                join core.series s using(series_id)
                where s.code=:code
                order by o.ts
            """), c, params={"code": code})
        if df.empty:
            continue

        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.dropna(subset=["value"]).sort_values("ts")
        v = df["value"].astype(float)
        z = _zscore(v)

        payload[code] = {
            "ts": [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "raw": [float(x) if x is not None else None for x in v],
            "z":   [float(x) if x is not None else None for x in z],
        }
        if default_code is None or code == "ICSA":
            default_code = code

    if not payload:
        raise RuntimeError("Нет макро-рядов (asset_class='macro').")

    if default_code is None:
        default_code = sorted(payload.keys())[0]

    return payload, default_code
