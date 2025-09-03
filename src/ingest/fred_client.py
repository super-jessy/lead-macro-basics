import os
import io
import pandas as pd
import requests

FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"

def _fetch_json(series_id: str, start: str, api_key: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    r = requests.get(FRED_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    obs = data.get("observations", [])
    if not obs:
        return pd.DataFrame(columns=["ts", "value"])
    df = pd.DataFrame(obs).rename(columns={"date": "ts"})
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts", "value"])
    return df[["ts", "value"]]

def _fetch_csv(series_id: str, start: str) -> pd.DataFrame:
    r = requests.get(FRED_CSV, params={"id": series_id}, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    if "DATE" not in df.columns or series_id not in df.columns:
        raise RuntimeError(f"FRED CSV не вернул ожидаемые колонки для {series_id}")
    df = df.rename(columns={"DATE": "ts", series_id: "value"})
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts"])
    if start:
        start_ts = pd.Timestamp(start, tz="UTC")
        df = df[df["ts"] >= start_ts]
    df = df.dropna(subset=["value"])
    return df[["ts", "value"]]

def fetch_fred_series(series_id: str, start: str) -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if api_key:
        # При наличии ключа — ТОЛЬКО JSON и с явной ошибкой, если что-то не так.
        df = _fetch_json(series_id, start, api_key)
        if df is None or df.empty:
            raise RuntimeError(f"FRED JSON пуст для {series_id} (проверь series_id и/или дату start).")
        return df
    # Без ключа — CSV fallback
    return _fetch_csv(series_id, start)

