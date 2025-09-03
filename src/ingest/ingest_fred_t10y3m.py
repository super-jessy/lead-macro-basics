import pandas as pd
from pandas_datareader import data as pdr
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from src.core.db import get_engine

FRED_CODE = "T10Y3M"   # 10Y - 3M Treasury spread
START = "2016-01-01"

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

def download_fred_series(code: str, start: str) -> pd.DataFrame:
    s = pdr.DataReader(code, "fred", start=start)
    if s is None or s.empty:
        raise RuntimeError(f"FRED вернул пусто по {code}")
    if isinstance(s, pd.DataFrame) and s.shape[1] == 1:
        s = s.iloc[:, 0]
    s = s.astype("float64")
    s.index = pd.to_datetime(s.index, utc=True)
    df = s.reset_index().rename(columns={"DATE": "ts", code: "value"})
    df = df[["ts", "value"]]
    df = df[df["ts"].notna()]
    if df.empty:
        raise RuntimeError("После нормализации df пуст")
    return df

def upsert_observations(conn, series_id: int, df: pd.DataFrame, chunk: int = 1000):
    md = MetaData()
    obs = Table("observation", md, autoload_with=conn, schema="core")
    ins = insert(obs)
    on_conflict = ins.on_conflict_do_update(
        index_elements=[obs.c.series_id, obs.c.ts],
        set_={
            "value": ins.excluded.value,
            "asof": text("now()"),
        },
    )
    rows = df.to_dict(orient="records")
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        payload = []
        for r in batch:
            ts = r.get("ts")
            val = r.get("value")
            if ts is None or val is None:
                continue
            payload.append({"series_id": series_id, "ts": ts, "value": float(val)})
        if payload:
            conn.execute(on_conflict, payload)
            total += len(payload)
    return total

def main():
    eng = get_engine()
    with eng.begin() as conn:
        fred_id = upsert_source(conn, "FRED")
        series_id = upsert_series(conn, fred_id, FRED_CODE, asset_class="macro", freq="D")
        df = download_fred_series(FRED_CODE, START)
        n = upsert_observations(conn, series_id, df)
        print(f"[OK] Загрузили/обновили {n} наблюдений для {FRED_CODE}")

if __name__ == "__main__":
    main()
