# src/reports/create_report.py

import os
import json
from string import Template
from datetime import datetime, timezone
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text

from src.core.db import get_engine


# ===================== DB LOADERS =====================

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
    Payload для JS:
    {
      CODE: { ts:[iso], raw:[float], z:[float] },
      ...
    }, default_code
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


# ===================== HEATMAP & IC HELPERS =====================

def _to_monthly_last(s: pd.Series) -> pd.Series:
    # month-end (НЕ 'M', чтобы избежать FutureWarning)
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


def load_spx_monthly_returns() -> pd.Series:
    spx_df, _ = load_spx_from_db()
    px = spx_df.set_index("ts")["px"].astype(float).sort_index()
    px_m = _to_monthly_last(px).dropna()
    ret_m = np.log(px_m).diff().dropna()
    return ret_m


def load_macro_monthly(code: str) -> pd.Series:
    eng = get_engine()
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
        return pd.Series(dtype=float)
    s = df.set_index(pd.to_datetime(df["ts"], utc=True))["value"].astype(float).sort_index()
    return _to_monthly_last(s).dropna()


def build_heatmap_matrix(lag_min: int = -12, lag_max: int = 12, min_obs: int = 24) -> pd.DataFrame:
    ret_m = load_spx_monthly_returns()
    codes = get_macro_codes_from_db()
    lags = list(range(lag_min, lag_max + 1))
    rows = []
    for code in codes:
        m = load_macro_monthly(code)
        if m.empty:
            rows.append([np.nan] * len(lags))
            continue
        z = _zscore(m)
        rows.append([_corr_at_lag(ret_m, z, L, min_obs=min_obs) for L in lags])
    return pd.DataFrame(rows, index=codes, columns=lags)


def build_ic_payload(lag_min: int = -12, lag_max: int = 12, min_obs: int = 24) -> Dict[str, dict]:
    """
    Возвращает словарь: { CODE: { 'lags': [...], 'ic': [...] }, ... }
    где ic[lag] = corr( next_ret, z.shift(lag) )
    """
    ret_m = load_spx_monthly_returns()
    next_ret = ret_m.shift(-1)  # доходность следующего месяца
    lags = list(range(lag_min, lag_max + 1))

    payload: Dict[str, dict] = {}
    for code in get_macro_codes_from_db():
        m = load_macro_monthly(code)
        if m.empty:
            payload[code] = {"lags": lags, "ic": [np.nan] * len(lags)}
            continue
        z = _zscore(m)
        vals = []
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


# ===================== PLOTS =====================

def build_timeseries_fig(
    spx_df: pd.DataFrame,
    spx_code: str,
    code: str,
    macro_payload: Dict[str, dict],
    use_z: bool = False,
    lag: int = 0,
) -> go.Figure:
    """Базовая фигура: S&P (лог) + выбранный индикатор (лаг применим на JS-стороне)."""
    spx = spx_df.copy()
    spx["px"] = spx["px"].astype(float)
    spx["log_px"] = np.log(spx["px"]).replace([np.inf, -np.inf], np.nan)

    x_spx = spx["ts"].tolist()
    y_spx = spx["log_px"].astype(float).tolist()

    m = macro_payload[code]
    x_macro = m["ts"]
    y_macro = m["z"] if use_z else m["raw"]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=x_spx, y=y_spx, name=f"{spx_code} (log)", mode="lines", line=dict(width=2.2))
    )
    fig.add_trace(
        go.Scatter(
            x=x_macro,
            y=y_macro,
            name=code + (" (z)" if use_z else ""),
            mode="lines",
            line=dict(width=1.5),
            yaxis="y2",
        )
    )

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
        xaxis=dict(
            title="Лаг (месяцы)  —  <0: индикатор опережает  |  >0: запаздывает",
            dtick=2,
        ),
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
        xaxis=dict(
            title="Лаг (месяцы)  —  <0: индикатор опережает  |  >0: запаздывает",
            dtick=2,
        ),
        yaxis=dict(title="IC", tickformat=".2f"),
        margin=dict(l=70, r=40, t=60, b=50),
        template="plotly_white",
    )
    return fig


# ===================== HTML TEMPLATE =====================

PAGE_TEMPLATE = Template(
    r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>DELTA TERMINAL - MVP</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin:16px; }
    .tabs { display:flex; gap:8px; margin:8px 0 12px; flex-wrap:wrap; }
    .tab { padding:6px 10px; border:1px solid #ccc; border-radius:8px; cursor:pointer; user-select:none; }
    .tab.active { background:#1f77b4; color:#fff; border-color:#1f77b4; }
    .panel { display:none; }
    .panel.active { display:block; }
    .controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin:8px 0 12px; }
    .muted { font-size:12px; opacity:.7; }
    #fig_timeseries, #fig_heatmap, #fig_ic { height: 70vh; }
    #errorBox { color:#b00020; white-space: pre-wrap; background:#ffecec; border:1px solid #ffb3b3; padding:8px 10px; border-radius:8px; display:none; }
    .plotly .gl-container .gl-error-message { display: none !important; }
  </style>
</head>
<body>

  <h2 style="margin:4px 0 12px 0;">DELTA TERMINAL - MVP</h2>
  <div id="errorBox"></div>

  <div class="tabs">
    <div class="tab active" data-target="panel-timeseries">Timeseries</div>
    <div class="tab" data-target="panel-heatmap">Heatmap</div>
    <div class="tab" data-target="panel-ic">IC</div>
    <div class="tab" data-target="panel-models">Models (soon)</div>
  </div>

  <div id="panel-timeseries" class="panel active">
    <div class="controls">
      <label>Индикатор:
        <select id="macroSelect" style="margin-left:6px; padding:2px 6px;"></select>
      </label>
      <label>Нормировка:
        <input type="checkbox" id="normChk" style="transform: translateY(2px); margin:0 6px;"> z-score
      </label>
      <label>Лаг (мес): <span id="lagVal">0</span>
        <input type="range" id="lagRange" min="-12" max="12" step="1" value="0" style="vertical-align: middle; margin-left:8px;">
      </label>
      <span class="muted">Положительный лаг сдвигает макро вправо (позже).</span>
    </div>
    <div id="fig_timeseries"></div>
  </div>

  <div id="panel-heatmap" class="panel">
    <div id="fig_heatmap"></div>
  </div>

  <div id="panel-ic" class="panel">
    <div class="controls">
      <label for="icSelect">Индикатор:</label>
      <select id="icSelect"></select>
    </div>
    <div id="fig_ic"></div>
  </div>

  <div id="panel-models" class="panel">
    <p class="muted">Здесь появится переключатель моделей и метрики (Sharpe, hit-rate и т.д.).</p>
  </div>

  <script>
    function showError(e) {
      const box = document.getElementById('errorBox');
      box.style.display = 'block';
      box.textContent = (e && e.stack) ? e.stack : String(e);
      console.error(e);
    }

    try {
      const spxCode      = "${spx_code}";
      const macroData    = ${macro_payload_json};
      const defaultCode  = "${default_code}";
      const tsFigureSpec = ${ts_fig_json};
      const hmFigureSpec = ${hm_fig_json};
      const icPayload    = ${ic_payload_json};
      const icFigureSpec = ${ic_fig_json};

      // ----- вкладки -----
      document.querySelectorAll('.tab').forEach(t => {
        t.addEventListener('click', () => {
          document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
          document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
          t.classList.add('active');
          document.getElementById(t.dataset.target).classList.add('active');

          if (t.dataset.target === 'panel-heatmap' && !window.__hmRendered) {
            Plotly.newPlot('fig_heatmap', hmFigureSpec.data, hmFigureSpec.layout, {responsive:true});
            window.__hmRendered = true;
          }
          if (t.dataset.target === 'panel-ic' && !window.__icRendered) {
            Plotly.newPlot('fig_ic', icFigureSpec.data, icFigureSpec.layout, {responsive:true});
            window.__icRendered = true;
            updateIC(icSelect.value);
          }
        });
      });

      // ----- селект Timeseries -----
      const sel = document.getElementById('macroSelect');
      Object.keys(macroData).sort().forEach(code => {
        const opt = document.createElement('option');
        opt.value = code; opt.textContent = code;
        if (code === defaultCode) opt.selected = true;
        sel.appendChild(opt);
      });

      // ----- селект IC -----
      const icSelect = document.getElementById("icSelect");
      Object.keys(macroData).sort().forEach(code => {
        const opt = document.createElement("option");
        opt.value = code;
        opt.textContent = code;
        icSelect.appendChild(opt);
      });
      icSelect.value = defaultCode;
      icSelect.addEventListener("change", () => {
        updateIC(icSelect.value);
      });

      // ----- первичная отрисовка ряда -----
      Plotly.newPlot('fig_timeseries', tsFigureSpec.data, tsFigureSpec.layout, {responsive:true});

      // ======= JS utils =======
      function addMonthsISO(iso, m) {
        const d = new Date(iso);
        let year = d.getUTCFullYear();
        let month = d.getUTCMonth();
        const day = d.getUTCDate();
        month += m; year += Math.floor(month/12); month = ((month%12)+12)%12;
        const nd = new Date(Date.UTC(year, month, 1));
        const daysInMonth = new Date(Date.UTC(year, month+1, 0)).getUTCDate();
        nd.setUTCDate(Math.min(day, daysInMonth));
        return nd.toISOString();
      }
      function withLag(tsArr, m){ if(!m) return tsArr; return tsArr.map(t => addMonthsISO(t, m)); }

      function currentState() {
        const code = sel.value;
        const z = document.getElementById('normChk').checked;
        const lag = parseInt(document.getElementById('lagRange').value, 10) || 0;
        return { code, z, lag };
      }
      function legendName(st) {
        const suffix = [];
        if (st.z) suffix.push('z');
        if (st.lag) suffix.push('lag ' + (st.lag>0? '+'+st.lag: st.lag) + 'm');
        return st.code + (suffix.length? ' ('+suffix.join(', ')+')' : '');
      }
      function applyState(st) {
        const rec = macroData[st.code]; if(!rec) return;
        const y = st.z ? rec.z : rec.raw;
        const x = withLag(rec.ts, st.lag);
        const nm = legendName(st);
        try {
          // trace 0 = SPX, trace 1 = макро
          Plotly.restyle('fig_timeseries', { x:[x], y:[y], name:[nm], 'yaxis':['y2'] }, [1]);
          Plotly.relayout('fig_timeseries', {
            'title.text': spxCode + ' (лог) и ' + nm,
            'yaxis2.title.text': st.code + (st.z ? ' (z-score)' : '')
          });
          document.getElementById('lagVal').textContent = String(st.lag);
        } catch (e) { showError(e); }
      }

      function updateIC(code){
        const item = icPayload[code];
        if (!item) return;
        if (!window.__icRendered) return; // вкладка IC ещё не открыта
        try {
          Plotly.react('fig_ic', [{ x: item.lags, y: item.ic, mode: 'lines+markers', name: 'IC' }], {
            ...icFigureSpec.layout,
            title: {text: 'Information Coefficient (next-month return) — ' + code}
          }, {responsive:true});
        } catch(e){ showError(e); }
      }

      // события UI
      sel.addEventListener('change', ()=>{ const st=currentState(); applyState(st); });
      document.getElementById('normChk').addEventListener('change', ()=>applyState(currentState()));
      document.getElementById('lagRange').addEventListener('input', ()=>applyState(currentState()));

      // стартовое состояние
      applyState(currentState());

    } catch (e) {
      showError(e);
    }
  </script>

</body>
</html>
"""
)


# ===================== MAIN =====================

def main():
    # данные
    spx_df, spx_code = load_spx_from_db()
    macro_payload, default_code = load_macro_series_payload()

    # фигуры (как dict-спеки — НЕ строки)
    ts_fig = build_timeseries_fig(spx_df, spx_code, default_code, macro_payload, use_z=False, lag=0)
    ts_fig_spec = json.loads(ts_fig.to_json())

    corr_mat = build_heatmap_matrix(lag_min=-12, lag_max=12, min_obs=24)
    hm_fig = build_heatmap_fig(corr_mat)
    hm_fig_spec = json.loads(hm_fig.to_json())

    ic_payload = build_ic_payload(lag_min=-12, lag_max=12, min_obs=24)
    ic_first_fig = build_ic_fig(default_code, ic_payload)
    ic_fig_spec = json.loads(ic_first_fig.to_json())

    # сборка html
    os.makedirs("output", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join("output", f"lead_macro_platform_{ts}.html")

    html = PAGE_TEMPLATE.substitute(
        spx_code=spx_code,
        macro_payload_json=json.dumps(macro_payload, ensure_ascii=False),
        default_code=default_code,
        ts_fig_json=json.dumps(ts_fig_spec, ensure_ascii=False),
        hm_fig_json=json.dumps(hm_fig_spec, ensure_ascii=False),
        ic_payload_json=json.dumps(ic_payload, ensure_ascii=False),
        ic_fig_json=json.dumps(ic_fig_spec, ensure_ascii=False),
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Платформа сохранена: {path}")


if __name__ == "__main__":
    main()
