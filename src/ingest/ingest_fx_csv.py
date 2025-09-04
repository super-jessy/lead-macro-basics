# src/ingest/ingest_fx_csv.py
from __future__ import annotations

import os
import glob
from typing import Iterable

import pandas as pd
from sqlalchemy import text

from src.core.db import get_engine

# формат из твоих файлов: 1993.04.27,00:00,0.7201,0.7265,0.7130,0.7150,2191
DATE_FMT = "%Y.%m.%d"
TIME_FMT = "%H:%M"


def infer_asset_class(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("XAU") or s.startswith("XAG"):
        return "metal"
    if len(s) == 6:
        return "fx"
    return "other"


def symbol_from_path(path: str) -> str:
    """'EURUSD D1.csv' -> 'EURUSD'"""
    base = os.path.basename(path)
    sym = base.split(" ")[0].upper()
    sym = sym.replace(".CSV", "").replace(".csv", "")
    return sym


def read_csv_one(path: str) -> pd.DataFrame:
    """
    Ожидаем CSV без заголовка:
    date,time,open,high,low,close,volume
    """
    df = pd.read_csv(
        path,
        header=None,
        names=["date", "time", "open", "high", "low", "close", "volume"],
    )

    # datetime (UTC)
    ts = pd.to_datetime(
        df["date"] + " " + df["time"],
        format=f"{DATE_FMT} {TIME_FMT}",
        utc=True,
        errors="coerce",
    )
    df = df.assign(ts=ts).dropna(subset=["ts"])

    # типы
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # отсеиваем строки без OHLC
    df = df.dropna(subset=["open", "high", "low", "close"])

    # нормируем колонки
    df = df[["ts", "open", "high", "low", "close", "volume"]].sort_values("ts")
    return df


def upsert_source(conn, name: str) -> int:
    sql = text("""
        INSERT INTO core.source (name) VALUES (:name)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING source_id;
    """)
    return conn.execute(sql, {"name": name}).scalar()


def upsert_series(conn, source_id: int, code: str, asset_class: str, freq: str = "D") -> int:
    sql = text("""
        INSERT INTO core.series (source_id, code, asset_class, freq)
        VALUES (:source_id, :code, :asset_class, :freq)
        ON CONFLICT (code) DO UPDATE
          SET source_id  = EXCLUDED.source_id,
              asset_class= EXCLUDED.asset_class,
              freq       = EXCLUDED.freq
        RETURNING series_id;
    """)
    params = {
        "source_id": source_id,
        "code": code,
        "asset_class": asset_class,
        "freq": freq,
    }
    return conn.execute(sql, params).scalar()


def upsert_prices(conn, series_id: int, df: pd.DataFrame) -> int:
    """
    Требуемые колонки: ts, open, high, low, close, (опционально volume).
    adj_close проставляем = close.
    """
    df = df.copy()

    # гарантируем наличие столбцов
    for col in ["ts", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None

    # типы
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # adj_close = close
    df["adj_close"] = df["close"]

    # выбрасываем пустые по ts/close
    df = df.dropna(subset=["ts", "close"])

    if df.empty:
        return 0

    payload = []
    for r in df.itertuples(index=False):
        payload.append({
            "series_id": series_id,
            "ts":        r.ts,
            "open":      getattr(r, "open", None),
            "high":      getattr(r, "high", None),
            "low":       getattr(r, "low", None),
            "close":     getattr(r, "close", None),
            "adj_close": getattr(r, "adj_close", getattr(r, "close", None)),
            "volume":    getattr(r, "volume", None),
        })

    # ВАЖНО: только :named-плейсхолдеры, без %(...)
    from sqlalchemy import text
    sql = text("""
        INSERT INTO core.price
          (series_id, ts, open, high, low, close, adj_close, volume)
        VALUES
          (:series_id, :ts, :open, :high, :low, :close, :adj_close, CAST(:volume AS BIGINT))
        ON CONFLICT (series_id, ts) DO UPDATE
          SET open      = EXCLUDED.open,
              high      = EXCLUDED.high,
              low       = EXCLUDED.low,
              close     = EXCLUDED.close,
              adj_close = EXCLUDED.adj_close,
              volume    = EXCLUDED.volume;
    """)

    BATCH = 1000
    total = 0
    for i in range(0, len(payload), BATCH):
        conn.execute(sql, payload[i:i+BATCH])
        total += len(payload[i:i+BATCH])
    return total


def ingest_paths(paths: Iterable[str]) -> None:
    eng = get_engine()
    ok = fail = 0
    for p in paths:
        sym = symbol_from_path(p)
        try:
            df = read_csv_one(p)
            if df.empty:
                print(f"[WARN] {sym}: пусто (после парсинга) — файл: {p}")
                fail += 1
                continue

            asset_class = infer_asset_class(sym)
            with eng.begin() as conn:  # отдельная транзакция на файл
                src_id = upsert_source(conn, "CSV")
                series_id = upsert_series(conn, src_id, sym, asset_class=asset_class, freq="D")
                n = upsert_prices(conn, series_id, df)
            print(f"[OK] {sym}: загружено/обновлено {n} баров из {os.path.basename(p)} (asset_class={asset_class})")
            ok += 1
        except Exception as e:
            print(f"[ERR] {p}: {e}")
            fail += 1
    print(f"Итог: OK={ok}, ERR={fail}")


def main():
    """
    По умолчанию ищем CSV тут: data/csv/*.csv
    Можно передать путь/глоб через переменную окружения FX_GLOB, например:
      FX_GLOB='data/csv/* D1.csv' python -m src.ingest.ingest_fx_csv
    """
    glob_pat = os.environ.get("FX_GLOB", "data/csv/*.csv")
    paths = sorted(glob.glob(glob_pat))
    if not paths:
        print(f"[WARN] Файлы не найдены по шаблону: {glob_pat}")
        return
    ingest_paths(paths)


if __name__ == "__main__":
    main()
