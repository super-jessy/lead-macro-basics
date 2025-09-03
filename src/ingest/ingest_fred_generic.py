import os
import sys
import json
import yaml
import pandas as pd
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from src.core.db import get_engine
from src.ingest.fred_client import fetch_fred_series

def upsert_source(conn, name: str) -> int:
    sql = """
    INSERT INTO core.source (name) VALUES (:name)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING source_id;
    """
    return conn.execute(text(sql), {"name": name}).scalar()

def upsert_series(conn, source_id: int, code: str, asset_class: str, freq: str, tz: str="UTC") -> int:
    sql = """
    INSERT INTO core.series (source_id, code, asset_class, freq, tz)
    VALUES (:source_id, :code, :asset_class, :freq, :tz)
    ON CONFLICT (source_id, code) DO UPDATE SET
        asset_class = EXCLUDED.asset_class,
        freq = EXCLUDED.freq,
        tz = EXCLUDED.tz
    RETURNING series_id;
    """
    return conn.execute(
        text(sql),
        {"source_id": source_id, "code": code, "asset_class": asset_class, "freq": freq, "tz": tz},
    ).scalar()

def upsert_observations(conn, series_id: int, df: pd.DataFrame, chunk: int = 1000):
    md = MetaData()
    obs = Table("observation", md, autoload_with=conn, schema="core")
    ins = insert(obs)
    on_conflict = ins.on_conflict_do_update(
        index_elements=[obs.c.series_id, obs.c.ts],
        set_={"value": ins.excluded.value, "asof": text("now()")},
    )
    rows = df.to_dict(orient="records")
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        payload = []
        for r in batch:
            ts = r.get("ts"); val = r.get("value")
            if ts is None or val is None:
                continue
            payload.append({"series_id": series_id, "ts": ts, "value": float(val)})
        if payload:
            conn.execute(on_conflict, payload)
            total += len(payload)
    return total

def run_from_config(cfg_path: str):
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)
    start = cfg.get("start", "2016-01-01")
    series = cfg.get("series", [])
    if not series:
        print("[WARN] В конфиге нет серий")
        return

    eng = get_engine()
    ok, warn = 0, 0
    with eng.begin() as conn:
        fred_id = upsert_source(conn, "FRED")
        for item in series:
            code = str(item["code"]).strip()
            freq = item.get("freq", "ME")
            try:
                df = fetch_fred_series(code, start)
                if df is None or df.empty:
                    print(f"[WARN] {code}: пустой ответ")
                    warn += 1
                    continue
                # приведение и чистка
                df = df.copy()
                df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df.dropna(subset=["ts", "value"])
                if df.empty:
                    print(f"[WARN] {code}: пусто после нормализации")
                    warn += 1
                    continue

                sid = upsert_series(conn, fred_id, code, asset_class="macro", freq=freq)
                n = upsert_observations(conn, sid, df)
                print(f"[OK] {code}: загружено/обновлено {n} наблюдений")
                ok += 1
            except Exception as e:
                print(f"[WARN] {code}: ошибка {e}")
                warn += 1
    print(f"Итог: OK={ok}, WARN={warn}")

def main():
    cfg_path = "config/fred_series.yaml"  # <-- здесь
    if len(sys.argv) > 1:
        cfg_path = sys.argv[1]
    run_from_config(cfg_path)

if __name__ == "__main__":
    main()
