# src/reports/markets.py
from __future__ import annotations

from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import text

from src.core.db import get_engine


def get_instrument_codes() -> List[str]:
    """
    Возвращает список кодов инструментов, у которых есть цены в core.price.
    Берём классы: equity, fx, metal (индекс, валюты, металлы).
    """
    sql = text("""
        select distinct s.code
        from core.series s
        join core.price p using(series_id)
        where s.asset_class in ('equity','fx','metal')
        order by s.code
    """)
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(sql).fetchall()
    return [r[0] for r in rows]


def _load_one_price(code: str) -> pd.DataFrame:
    """
    Загружает OHLC для одного инструмента.
    Возвращает колонки: ts(UTC), open, high, low, close
    """
    sql = text("""
        select p.ts, p.open, p.high, p.low, coalesce(p.adj_close, p.close) as close
        from core.price p
        join core.series s using(series_id)
        where s.code = :code
        order by p.ts
    """)
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql(sql, c, params={"code": code})
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    # числовые типы
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["ts", "close"]).sort_values("ts")
    return df[["ts", "open", "high", "low", "close"]]


def load_price_payload() -> Tuple[Dict[str, dict], str]:
    """
    Собирает JSON-payload для вкладки Markets И (опционально) для Timeseries.
    Формат:
      {
        CODE: {
          "ts":   [iso...],
          "open": [float...],
          "high": [...],
          "low":  [...],
          "close":[...]
        },
        ...
      }, default_code
    """
    codes = get_instrument_codes()
    payload: Dict[str, dict] = {}

    for code in codes:
        df = _load_one_price(code)
        if df.empty:
            continue
        payload[code] = {
            "ts":    [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "open":  [float(x) if x is not None else None for x in df["open"]],
            "high":  [float(x) if x is not None else None for x in df["high"]],
            "low":   [float(x) if x is not None else None for x in df["low"]],
            "close": [float(x) if x is not None else None for x in df["close"]],
        }

    # дефолт: стараемся выбрать индекс, иначе — первый доступный
    default_code = None
    if "^GSPC" in payload:
        default_code = "^GSPC"
    elif "SPY" in payload:
        default_code = "SPY"
    elif payload:
        default_code = sorted(payload.keys())[0]
    else:
        default_code = ""  # пусто

    return payload, default_code
