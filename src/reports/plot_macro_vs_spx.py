import os
import sys
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone
from sqlalchemy import text
from src.core.db import get_engine

def load_spx_from_db():
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
        raise RuntimeError("В БД нет ни ^GSPC, ни SPY")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    # если в базе оба — берём ^GSPC
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    df = df[df["asset_code"] == preferred].dropna(subset=["px"])
    return df[["ts","px"]], preferred

def load_macro_from_db(code: str):
    eng = get_engine()
    with eng.connect() as c:
        q = text("""
            select o.ts, o.value
            from core.observation o
            join core.series s using(series_id)
            where s.code = :code
            order by o.ts
        """)
        df = pd.read_sql(q, c, params={"code": code})
    if df.empty:
        raise RuntimeError(f"В БД нет серии {code}")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.dropna(subset=["value"])
    return df

def make_chart(spx: pd.DataFrame, macro: pd.DataFrame, macro_code: str, spx_code: str):
    spx = spx.copy()
    spx["log_px"] = np.where(spx["px"] > 0, np.log(spx["px"]), np.nan)

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=spx["ts"], y=spx["log_px"],
        name=f"{spx_code} (log)",
        mode="lines",
        line=dict(width=2.2)
    ))
    fig.add_trace(go.Scatter(
        x=macro["ts"], y=macro["value"],
        name=f"{macro_code}",
        mode="lines",
        line=dict(width=1.5),
        yaxis="y2"
    ))

    fig.update_layout(
        title=f"{spx_code} (log) и {macro_code}",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=70, b=40),
        xaxis=dict(title="Дата"),
        yaxis=dict(title=f"log({spx_code})", tickformat=".2f"),
        yaxis2=dict(title=f"{macro_code}", overlaying="y", side="right", zeroline=True, zerolinewidth=1),
        template="plotly_white",
        hovermode="x unified",
    )
    return fig

def save_html(fig, macro_code: str, out_dir="output"):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"{macro_code}_vs_spx_{ts}.html")
    fig.write_html(path, include_plotlyjs="cdn")
    print(f"[OK] Сохранено: {path}")
    return path

def main():
    if len(sys.argv) < 2:
        print("Использование: python -m src.reports.plot_macro_vs_spx <FRED_CODE>")
        print("Например: python -m src.reports.plot_macro_vs_spx ICSA")
        sys.exit(1)
    macro_code = sys.argv[1].strip().upper()
    spx, spx_code = load_spx_from_db()
    macro = load_macro_from_db(macro_code)
    fig = make_chart(spx, macro, macro_code, spx_code)
    save_html(fig, macro_code)

if __name__ == "__main__":
    main()
