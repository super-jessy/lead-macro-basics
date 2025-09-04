# src/reports/analytics.py
from __future__ import annotations

from typing import Dict, List
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.core.db import get_engine
from src.reports.loaders import get_macro_codes_from_db


# --------- helpers ---------
def _to_monthly_last(s: pd.Series) -> pd.Series:
    # month-end (ME), чтобы не ловить FutureWarning от 'M'
    return s.sort_index().resample("ME").last()


def _zscore(x: pd.Series) -> pd.Series:
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if pd.isna(sd) or sd == 0:
        return pd.Series(np.zeros(len(x)), index=x.index)
    return (x - mu) / sd


def _corr_at_lag(ret: pd.Series, macro_m: pd.Series, lag_m: int, min_obs: int = 24) -> float:
    shifted = macro_m.shift(lag_m)
    df = pd.concat([ret.rename("ret"), shifted.rename("macro")], axis=1, join="inner").dropna()
    if len(df) < min_obs:
        return np.nan
    return float(df["ret"].corr(df["macro"]))


# --------- SPX monthly returns (лог-доходности) ---------
def load_spx_monthly_returns() -> pd.Series:
    """
    Берём ^GSPC (если нет — SPY), coalesce(adj_close, close),
    агрегируем по ME и считаем лог-доходности (diff логов).
    """
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql(
            text("""
                select p.ts,
                       p.close, p.adj_close,
                       s.code as asset_code
                from core.price p
                join core.series s using(series_id)
                where s.code in ('^GSPC','SPY')
                order by p.ts
            """),
            c,
        )

    if df.empty:
        return pd.Series(dtype=float)

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    # приоритет ^GSPC
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    df = df[df["asset_code"] == preferred].dropna(subset=["close"]).sort_values("ts")

    px = df["adj_close"].fillna(df["close"]).astype(float).to_numpy()
    s = pd.Series(px, index=pd.to_datetime(df["ts"], utc=True))
    px_m = _to_monthly_last(s).dropna()
    ret_m = np.log(px_m).diff().dropna()
    return ret_m


# --------- Macro monthly ---------
def load_macro_monthly(code: str) -> pd.Series:
    eng = get_engine()
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
        return pd.Series(dtype=float)

    s = (
        pd.Series(df["value"].astype(float).to_numpy(),
                  index=pd.to_datetime(df["ts"], utc=True))
        .sort_index()
    )
    return _to_monthly_last(s).dropna()


# --------- Heatmap ---------
def build_heatmap_matrix(lag_min: int = -12, lag_max: int = 12, min_obs: int = 24) -> pd.DataFrame:
    ret_m = load_spx_monthly_returns()
    codes = get_macro_codes_from_db()
    lags = list(range(lag_min, lag_max + 1))

    rows = []
    for code in codes:
        m = load_macro_monthly(code)
        if m.empty or ret_m.empty:
            rows.append([np.nan] * len(lags))
            continue
        z = _zscore(m)
        rows.append([_corr_at_lag(ret_m, z, L, min_obs=min_obs) for L in lags])

    return pd.DataFrame(rows, index=codes, columns=lags)


# --------- Information Coefficient (IC) ---------
def build_ic_payload(lag_min: int = -12, lag_max: int = 12, min_obs: int = 24) -> Dict[str, dict]:
    """
    Возвращает { CODE: { 'lags': [...], 'ic': [...] }, ... }
    где ic[lag] = corr( next_ret, z.shift(lag) ).
    """
    ret_m = load_spx_monthly_returns()
    next_ret = ret_m.shift(-1)  # доходность следующего месяца
    lags = list(range(lag_min, lag_max + 1))

    payload: Dict[str, dict] = {}
    for code in get_macro_codes_from_db():
        m = load_macro_monthly(code)
        if m.empty or ret_m.empty:
            payload[code] = {"lags": lags, "ic": [np.nan] * len(lags)}
            continue

        z = _zscore(m)
        vals: List[float] = []
        for L in lags:
            aligned = pd.concat(
                [next_ret.rename("ret_next"), z.shift(L).rename("macro")],
                axis=1, join="inner"
            ).dropna()
            if len(aligned) < min_obs:
                vals.append(np.nan)
            else:
                vals.append(float(aligned["ret_next"].corr(aligned["macro"])))

        payload[code] = {"lags": lags, "ic": vals}

    return payload
