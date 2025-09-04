# src/reports/loaders.py
from typing import Dict, Tuple, List
import numpy as np
import pandas as pd
from sqlalchemy import text
from src.core.db import get_engine

def load_spx_from_db() -> Tuple[pd.DataFrame, str]:
    """Вернёт df с ценой индекса и код (^GSPC или SPY)."""
    eng = get_engine()
    with eng.connect() as c:
        q = text(
            """
            select p.ts, coalesce(p.adj_close, p.close) as px, s.code as asset_code
            from core.price p
            join core.series s using(series_id)
            where s.code in ('^GSPC','SPY')
            order by ts
            """
        )
        df = pd.read_sql(q, c)

    if df.empty:
        raise RuntimeError("В БД нет ни ^GSPC, ни SPY")

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    df = df[df["asset_code"] == preferred].dropna(subset=["px"]).sort_values("ts")
    return df[["ts", "px"]], preferred


def get_macro_codes_from_db() -> List[str]:
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(
            text(
                """
                select s.code
                from core.series s
                where s.asset_class='macro'
                  and exists (select 1 from core.observation o where o.series_id=s.series_id)
                order by s.code
                """
            )
        ).fetchall()
    return [r.code for r in rows]


def load_macro_series_payload() -> Tuple[Dict[str, dict], str]:
    """
    Готовим полезную нагрузку для фронта:
    { CODE: { ts:[iso], raw:[float], z:[float] }, ... }, default_code
    """
    eng = get_engine()
    payload: Dict[str, dict] = {}
    default_code: str | None = None

    for code in get_macro_codes_from_db():
        with eng.connect() as c:
            df = pd.read_sql(
                text(
                    """
                    select o.ts, o.value
                    from core.observation o
                    join core.series s using(series_id)
                    where s.code=:code
                    order by o.ts
                    """
                ),
                c,
                params={"code": code},
            )
        if df.empty:
            continue

        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.dropna(subset=["value"]).sort_values("ts")
        v = df["value"].astype(float).to_numpy()
        mu, sd = float(np.nanmean(v)), float(np.nanstd(v))
        z = np.zeros_like(v) if (sd == 0 or np.isnan(sd)) else (v - mu) / sd

        payload[code] = {
            "ts": [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "raw": [float(x) if x is not None else None for x in v],
            "z": [float(x) if x is not None else None for x in z],
        }
        if default_code is None or code == "ICSA":
            default_code = code

    if not payload:
        raise RuntimeError("Нет макро-рядов (asset_class='macro')")

    if default_code is None:
        default_code = sorted(payload.keys())[0]

    return payload, default_code
