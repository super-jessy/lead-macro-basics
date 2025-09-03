import os
from datetime import datetime, timezone
from typing import Dict, List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text

from src.core.db import get_engine


# -------------------- helpers --------------------

def _to_monthly_last(s: pd.Series) -> pd.Series:
    """Привести ряд к помесячному шагу (конец месяца, последнее наблюдение)."""
    s = s.sort_index()
    # берем последнее значение месяца; если в месяце нет значения — NaN (без агрессивного ffill)
    m = s.resample("M").last()
    return m


def _zscore(x: pd.Series) -> pd.Series:
    """Стандартная нормировка (z-score) со страховкой от деления на ноль."""
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if pd.isna(sd) or sd == 0:
        return pd.Series(np.zeros(len(x)), index=x.index)
    return (x - mu) / sd


def _corr_at_lag(ret: pd.Series, macro_m: pd.Series, lag_m: int, min_obs: int = 24) -> float:
    """
    Корреляция между месячными доходностями индекса и макро,
    где положительный lag_m сдвигает макро ВПРАВО (то есть макро запаздывает).
    Отрицательный лаг => макро опережает.
    """
    shifted = macro_m.shift(lag_m)
    df = pd.concat([ret.rename("ret"), shifted.rename("macro")], axis=1, join="inner").dropna()
    if len(df) < min_obs:
        return np.nan
    return float(df["ret"].corr(df["macro"]))


# -------------------- load from DB --------------------

def load_spx_monthly_returns() -> pd.Series:
    """Загрузить из БД ^GSPC/или SPY, привести к M и посчитать лог-доходности."""
    eng = get_engine()
    with eng.connect() as c:
        q = text("""
            select p.ts, coalesce(p.adj_close, p.close) as px, s.code as asset_code
            from core.price p
            join core.series s using(series_id)
            where s.code in ('^GSPC','SPY')
            order by ts
        """)
        df = pd.read_sql(q, c)

    if df.empty:
        raise RuntimeError("В БД нет цен ^GSPC или SPY")

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    # если загружены оба — используем ^GSPC
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    px = (
        df[df["asset_code"] == preferred]
        .dropna(subset=["px"])
        .set_index("ts")["px"]
        .sort_index()
    )

    px_m = _to_monthly_last(px).dropna()
    ret_m = np.log(px_m).diff().dropna()
    return ret_m


def load_macro_series_codes() -> List[str]:
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


def load_macro_monthly(code: str) -> pd.Series:
    """Вытянуть макро-ряд из БД, привести к M (последнее значение месяца)."""
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

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    s = df.set_index("ts")["value"].astype(float)
    m = _to_monthly_last(s).dropna()
    return m


# -------------------- main logic --------------------

def build_heatmap_matrix(lag_min: int = -12, lag_max: int = 12, min_obs: int = 24) -> pd.DataFrame:
    """
    Вернуть DataFrame: rows=macro codes, cols=lags, values=corr.
    Лаги в месяцах: отрицательные = макро опережает.
    """
    ret_m = load_spx_monthly_returns()
    codes = load_macro_series_codes()

    lags = list(range(lag_min, lag_max + 1))
    data = []

    for code in codes:
        macro_m = load_macro_monthly(code)
        if macro_m.empty:
            row = [np.nan] * len(lags)
        else:
            z = _zscore(macro_m)  # нормируем, чтобы разные масштабы были сопоставимы
            row = [_corr_at_lag(ret_m, z, L, min_obs=min_obs) for L in lags]
        data.append(row)

    mat = pd.DataFrame(data, index=codes, columns=lags)
    return mat


def save_heatmap_html(corr_mat: pd.DataFrame, out_dir: str = "output") -> str:
    os.makedirs(out_dir, exist_ok=True)
    # подготовка для красивой подписи оси X
    x_vals = corr_mat.columns.tolist()  # лаги (ints)
    y_vals = corr_mat.index.tolist()
    z_vals = corr_mat.values

    fig = go.Figure(
        data=go.Heatmap(
            z=z_vals,
            x=x_vals,
            y=y_vals,
            colorscale=[
                [0.0, "#313695"],
                [0.1, "#4575B4"],
                [0.2, "#74ADD1"],
                [0.3, "#ABD9E9"],
                [0.4, "#E0F3F8"],
                [0.5, "#FFFFFF"],
                [0.6, "#FEE090"],
                [0.7, "#FDAE61"],
                [0.8, "#F46D43"],
                [0.9, "#D73027"],
                [1.0, "#A50026"],
            ],
            zmid=0.0,
            colorbar=dict(title="Корреляция"),
            hovertemplate="Индикатор: %{y}<br>Лаг (мес): %{x}<br>Corr: %{z:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        title="Корреляция лог-доходностей S&P 500 с макро-индикаторами на лагах (месяцы)",
        xaxis=dict(title="Лаг (месяцы)  —  <0: индикатор опережает  |  >0: запаздывает", dtick=2),
        yaxis=dict(title="Индикатор"),
        margin=dict(l=120, r=50, t=70, b=60),
        template="plotly_white",
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"lagged_corr_heatmap_{ts}.html")
    fig.write_html(path, include_plotlyjs="cdn")
    print(f"[OK] Сохранено: {path}")
    return path


def main():
    # Можно варьировать диапазон лагов и обязательный минимум наблюдений
    corr_mat = build_heatmap_matrix(lag_min=-12, lag_max=12, min_obs=24)
    save_heatmap_html(corr_mat)


if __name__ == "__main__":
    main()
