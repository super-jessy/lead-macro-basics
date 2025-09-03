import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from src.core.db import get_engine

SPX_TICKER = "^GSPC"

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

def download_spx(start="2016-01-01"):
    import pandas as pd
    import yfinance as yf

    df = yf.download(SPX_TICKER, start=start, auto_adjust=False,
                     progress=False, threads=False, interval="1d")
    if df is None or df.empty:
        raise RuntimeError("yfinance вернул пустой датафрейм по ^GSPC")

    df = df.copy()

    # 1) Если пришёл MultiIndex колонок — вытащим срез по тикеру (^GSPC)
    if isinstance(df.columns, pd.MultiIndex):
        # найдём уровень, где сидит тикер
        found = False
        for lvl in range(df.columns.nlevels):
            try:
                if SPX_TICKER in df.columns.get_level_values(lvl):
                    df = df.xs(SPX_TICKER, axis=1, level=lvl)
                    found = True
                    break
            except Exception:
                pass
        if not found:
            # fallback: возьмём первый столбец каждого «поля»
            # (на случай, если структура вдруг иная)
            df = df.droplevel(-1, axis=1)

    # 2) Индекс -> UTC-даты и отдельная колонка ts
    df.index = pd.to_datetime(df.index, utc=True)
    df["ts"] = df.index

    # 3) Приведение имён и выбор нужных колонок
    df = df.rename(columns={
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adj_close", "Volume": "volume"
    })
    cols = ["ts", "open", "high", "low", "close", "adj_close", "volume"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"В данных не хватает колонок: {missing}. Колонки df: {list(df.columns)}")

    df = df[cols]
    df = df[df["ts"].notna()]

    if df.empty:
        raise RuntimeError("После нормализации df пуст")

    # Помечаем, какой код реально качали (полезно, если добавишь fallback на SPY)
    df.attrs["code"] = SPX_TICKER
    return df



def upsert_prices(conn, series_id: int, df: pd.DataFrame, chunk: int = 1000):
    from sqlalchemy import Table, MetaData
    from sqlalchemy.dialects.postgresql import insert

    md = MetaData()
    price = Table("price", md, autoload_with=conn, schema="core")

    ins = insert(price)
    on_conflict = ins.on_conflict_do_update(
        index_elements=[price.c.series_id, price.c.ts],
        set_={
            "open": ins.excluded.open,
            "high": ins.excluded.high,
            "low": ins.excluded.low,
            "close": ins.excluded.close,
            "adj_close": ins.excluded.adj_close,
            "volume": ins.excluded.volume,
        },
    )

    rows = df.to_dict(orient="records")
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        payload = []
        for r in batch:
            ts = r.get("ts")
            if ts is None:
                continue
            payload.append({
                "series_id": series_id,
                "ts": r.get("ts"),
                "open": float(r.get("open")) if r.get("open") is not None else None,
                "high": float(r.get("high")) if r.get("high") is not None else None,
                "low": float(r.get("low")) if r.get("low") is not None else None,
                "close": float(r.get("close")) if r.get("close") is not None else None,
                "adj_close": float(r.get("adj_close")) if r.get("adj_close") is not None else None,
                "volume": int(r.get("volume")) if r.get("volume") is not None else None,
            })
        if payload:
            conn.execute(on_conflict, payload)
            total += len(payload)
    return total


def main():
    eng = get_engine()
    with eng.begin() as conn:
        # 1) источники и серия
        yf_source_id = upsert_source(conn, "YF")
        spx_series_id = upsert_series(conn, yf_source_id, SPX_TICKER, asset_class="equity", freq="D")

        # 2) данные
        df = download_spx(start="2016-01-01")

        # 3) upsert
        n = upsert_prices(conn, spx_series_id, df)
        print(f"[OK] Загрузили/обновили {n} баров для {SPX_TICKER}")

if __name__ == "__main__":
    main()
