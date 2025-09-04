# src/reports/plots.py
from typing import Dict
import numpy as np
import pandas as pd
import plotly.graph_objects as go


def build_timeseries_fig(
    spx_df: pd.DataFrame,
    spx_code: str,
    code: str,
    macro_payload: Dict[str, dict],
    use_z: bool = False,
    lag: int = 0,
) -> go.Figure:
    """S&P (лог) + выбранный индикатор. Лаг двигаем на фронте (JS)."""
    spx = spx_df.copy()
    spx["px"] = spx["px"].astype(float)
    spx["log_px"] = np.log(spx["px"]).replace([np.inf, -np.inf], np.nan)

    x_spx = spx["ts"].tolist()
    y_spx = spx["log_px"].astype(float).tolist()

    m = macro_payload[code]
    x_macro = m["ts"]
    y_macro = m["z"] if use_z else m["raw"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x_spx, y=y_spx, name=f"{spx_code} (log)", mode="lines", line=dict(width=2.2)))
    fig.add_trace(go.Scatter(x=x_macro, y=y_macro, name=code + (" (z)" if use_z else ""), mode="lines",
                             line=dict(width=1.5), yaxis="y2"))

    fig.update_layout(
        title=f"{spx_code} (лог) и {code}" + (" (z-score)" if use_z else ""),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=70, b=40),
        xaxis=dict(title="Дата"),
        yaxis=dict(title=f"log({spx_code})", tickformat=".2f"),
        yaxis2=dict(title=code, overlaying="y", side="right"),
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


def build_heatmap_fig(corr_mat: pd.DataFrame) -> go.Figure:
    x_vals = corr_mat.columns.tolist()
    y_vals = corr_mat.index.tolist()
    z_vals = corr_mat.values

    fig = go.Figure(
        data=go.Heatmap(
            z=z_vals,
            x=x_vals,
            y=y_vals,
            colorscale=[
                [0.0, "#313695"], [0.1, "#4575B4"], [0.2, "#74ADD1"], [0.3, "#ABD9E9"], [0.4, "#E0F3F8"],
                [0.5, "#FFFFFF"], [0.6, "#FEE090"], [0.7, "#FDAE61"], [0.8, "#F46D43"], [0.9, "#D73027"],
                [1.0, "#A50026"],
            ],
            zmid=0.0,
            colorbar=dict(title="Корреляция"),
            hovertemplate="Индикатор: %{y}<br>Лаг (мес): %{x}<br>Corr: %{z:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Корреляция лог-доходностей S&P 500 с макро-индикаторами на лагах (месяцы)",
        xaxis=dict(title="Лаг (месяцы)  —  <0: опережает | >0: запаздывает", dtick=2),
        yaxis=dict(title="Индикатор"),
        margin=dict(l=120, r=50, t=70, b=60),
        template="plotly_white",
    )
    return fig


def build_ic_fig(code: str, ic_payload: Dict[str, dict]) -> go.Figure:
    item = ic_payload[code]
    lags = item["lags"]
    ic = item["ic"]

    fig = go.Figure(go.Scatter(x=lags, y=ic, mode="lines+markers", name="IC"))
    fig.add_hline(y=0, line=dict(width=1, dash="dot", color="#888"))

    fig.update_layout(
        title=f"Information Coefficient (next-month return) — {code}",
        xaxis=dict(title="Лаг (месяцы)  —  <0: опережает | >0: запаздывает", dtick=2),
        yaxis=dict(title="IC", tickformat=".2f"),
        margin=dict(l=70, r=40, t=60, b=50),
        template="plotly_white",
    )
    return fig
