import os
from datetime import datetime, timezone
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from src.core.db import get_engine

def load_spx_from_db():
    eng = get_engine()
    with eng.connect() as c:
        # пробуем ^GSPC, если нет — SPY как прокси
        q = text("""
            with spx as (
              select p.ts, coalesce(p.adj_close, p.close) as px
              from core.price p
              join core.series s using(series_id)
              where s.code = :code
            )
            select ts, px
            from spx
            order by ts
        """)
        for code in ("^GSPC", "SPY"):
            df = pd.read_sql(q, c, params={"code": code})
            if not df.empty:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
                df = df.dropna(subset=["px"])
                df["asset_code"] = code
                return df
    raise RuntimeError("В БД нет ни ^GSPC, ни SPY в core.price")

def load_t10y3m_from_db():
    eng = get_engine()
    with eng.connect() as c:
        q = text("""
            select o.ts, o.value
            from core.observation o
            join core.series s using(series_id)
            where s.code = 'T10Y3M'
            order by o.ts
        """)
        df = pd.read_sql(q, c)
        if df.empty:
            raise RuntimeError("В БД нет T10Y3M в core.observation")
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.dropna(subset=["value"])
        return df

def make_chart(spx: pd.DataFrame, yc: pd.DataFrame):
    # синхронизируем даты (левое джойнить не обязательно — просто рисуем по своим осям)
    # логарифмируем цену для левой оси
    spx_plot = spx.copy()
    spx_plot["log_px"] = (spx_plot["px"]).apply(lambda x: None if x is None or x <= 0 else x)
    spx_plot["log_px"] = spx_plot["log_px"].apply(lambda x: None if x is None else pd.Series([x]).apply('log').iloc[0])

    fig = go.Figure()

    # S&P 500 (лог-ось)
    fig.add_trace(go.Scatter(
        x=spx_plot["ts"], y=spx_plot["log_px"],
        name=f"{spx_plot['asset_code'].iloc[0]} (log)",
        mode="lines",
        line=dict(width=2.2)
    ))

    # T10Y3M (правая ось)
    fig.add_trace(go.Scatter(
        x=yc["ts"], y=yc["value"],
        name="T10Y3M (10Y-3M, %)",
        mode="lines",
        line=dict(width=1.5),
        yaxis="y2"
    ))

    fig.update_layout(
        title="S&P 500 (лог) и кривая доходности 10Y-3M (T10Y3M)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=70, b=40),
        xaxis=dict(title="Дата"),
        yaxis=dict(title="log(SPX)", tickformat=".2f"),
        yaxis2=dict(title="T10Y3M, п.п.", overlaying="y", side="right", zeroline=True, zerolinewidth=1),
        template="plotly_white",
        hovermode="x unified",
    )

    return fig

def save_html(fig, out_dir="output"):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"spx_t10y3m_{ts}.html")
    fig.write_html(path, include_plotlyjs="cdn")
    print(f"[OK] Отчёт сохранён: {path}")
    return path

def main():
    spx = load_spx_from_db()
    yc = load_t10y3m_from_db()
    fig = make_chart(spx, yc)
    save_html(fig)

if __name__ == "__main__":
    main()
