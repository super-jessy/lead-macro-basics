import os
import json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from src.core.db import get_engine
from string import Template


# ---------- загрузка данных из БД ----------

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
    codes = df["asset_code"].unique().tolist()
    preferred = "^GSPC" if "^GSPC" in codes else codes[0]
    df = df[df["asset_code"] == preferred].dropna(subset=["px"])
    return df[["ts", "px"]], preferred


def get_macro_codes_from_db():
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(text("""
            select s.series_id, s.code
            from core.series s
            where s.asset_class='macro'
              and exists (select 1 from core.observation o where o.series_id = s.series_id)
            order by s.code
        """)).fetchall()
    return [(r.series_id, r.code) for r in rows]


def load_macro_series_payload():
    """
    Возвращает:
      macro_payload = {
        CODE: { "ts": [...iso...], "raw": [...], "z": [...], "desc": "" }
      },
      default_code
    """
    eng = get_engine()
    payload = {}
    default_code = None
    for series_id, code in get_macro_codes_from_db():
        with eng.connect() as c:
            df = pd.read_sql(
                text("select ts, value from core.observation where series_id=:sid order by ts"),
                c, params={"sid": series_id}
            )
        if df.empty:
            continue
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.dropna(subset=["value"])
        # z-score (осторожно со std=0)
        v = df["value"].astype(float).values
        mu = float(np.nanmean(v))
        sd = float(np.nanstd(v))
        if sd == 0 or np.isnan(sd):
            z = np.zeros_like(v, dtype=float)
        else:
            z = (v - mu) / sd

        payload[code] = {
            "ts": [pd.Timestamp(t).isoformat() for t in df["ts"]],
            "raw": [float(x) if x is not None else None for x in v],
            "z":   [float(x) if x is not None else None for x in z],
        }

        if default_code is None or code == "ICSA":
            default_code = code

    if not payload:
        raise RuntimeError("В БД не найдено ни одного макро-ряда (asset_class='macro').")

    if default_code is None:
        default_code = sorted(payload.keys())[0]

    return payload, default_code


# ---------- построение графика ----------

def make_base_figure(spx_df: pd.DataFrame, spx_code: str, macro_code: str,
                     macro_ts, macro_y, title_suffix=""):
    spx = spx_df.copy()
    spx["log_px"] = np.where(spx["px"] > 0, np.log(spx["px"]), np.nan)

    fig = go.Figure()

    # SPX (лог)
    fig.add_trace(go.Scatter(
        x=spx["ts"], y=spx["log_px"],
        name=f"{spx_code} (log)",
        mode="lines",
        line=dict(width=2.2)
    ))

    # Macro (правaя ось)
    fig.add_trace(go.Scatter(
        x=macro_ts, y=macro_y,
        name=f"{macro_code}",
        mode="lines",
        line=dict(width=1.5),
        yaxis="y2"
    ))

    fig.update_layout(
        title=f"{spx_code} (лог) и {macro_code}{title_suffix}",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=70, b=40),
        xaxis=dict(title="Дата"),
        yaxis=dict(title=f"log({spx_code})", tickformat=".2f"),
        yaxis2=dict(title=f"{macro_code}", overlaying="y", side="right", zeroline=True, zerolinewidth=1),
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


from string import Template

def save_html(fig, macro_payload: dict, default_code: str, spx_code: str, out_dir="output", div_id="macro_chart"):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"interactive_macro_vs_spx_{ts}.html")

    # JS-код через string.Template, чтобы фигурные скобки JS не ломали подстановку
    js_tpl = Template(r"""
var CHART_ID = "${div_id}";
var macroData = ${macro_payload_json};
var defaultCode = "${default_code}";
var spxCode = "${spx_code}";

// Вставим контролы над графиком
(function() {
  var chartDiv = document.getElementById(CHART_ID);
  var wrap = chartDiv.parentElement;
  var controls = document.createElement('div');
  controls.style.margin = '8px 0 12px 0';
  controls.style.display = 'flex';
  controls.style.flexWrap = 'wrap';
  controls.style.gap = '12px';
  controls.style.alignItems = 'center';

  controls.innerHTML = `
    <label style="font: 14px sans-serif;">
      Индикатор:
      <select id="macroSelect" style="margin-left:6px; padding:2px 6px;"></select>
    </label>
    <label style="font: 14px sans-serif;">
      Нормировка:
      <input type="checkbox" id="normChk" style="transform: translateY(2px); margin:0 6px;"> z-score
    </label>
    <label style="font: 14px sans-serif;">
      Лаг (мес): <span id="lagVal">0</span>
      <input type="range" id="lagRange" min="-12" max="12" step="1" value="0" style="vertical-align: middle; margin-left:8px;">
    </label>
    <span style="font:12px sans-serif; opacity:.7;">Положительный лаг сдвигает макро вправо (позже).</span>
  `;
  wrap.insertBefore(controls, chartDiv);

  // заполним список кодов
  var sel = document.getElementById('macroSelect');
  Object.keys(macroData).sort().forEach(function(code){
     var opt = document.createElement('option');
     opt.value = code; opt.textContent = code;
     if (code === defaultCode) opt.selected = true;
     sel.appendChild(opt);
  });

  function addMonthsISO(iso, m) {
    var d = new Date(iso);
    var year = d.getUTCFullYear();
    var month = d.getUTCMonth();
    var day = d.getUTCDate();
    month += m;
    year += Math.floor(month / 12);
    month = ((month % 12) + 12) % 12;
    var nd = new Date(Date.UTC(year, month, 1));
    var daysInMonth = new Date(Date.UTC(year, month+1, 0)).getUTCDate();
    nd.setUTCDate(Math.min(day, daysInMonth));
    return nd.toISOString();
  }

  function withLag(tsArr, m) {
    if (!m) return tsArr;
    return tsArr.map(function(t) { return addMonthsISO(t, m); });
  }

  function currentState() {
    var code = sel.value;
    var z = document.getElementById('normChk').checked;
    var lag = parseInt(document.getElementById('lagRange').value, 10) || 0;
    return { code: code, z: z, lag: lag };
  }

  function legendName(st) {
    var suffix = [];
    if (st.z) suffix.push('z');
    if (st.lag) suffix.push('lag ' + (st.lag>0? '+'+st.lag: st.lag) + 'm');
    return st.code + (suffix.length? ' ('+suffix.join(', ')+')' : '');
  }

  function applyState(st) {
    var rec = macroData[st.code];
    if (!rec) return;
    var y = st.z ? rec.z : rec.raw;
    var x = withLag(rec.ts, st.lag);
    var nm = legendName(st);

    // Обновим линию индикатора (trace 1)
    Plotly.restyle(CHART_ID, { x: [x], y: [y], name: [nm] }, [1]);

    // Обновим подписи
    Plotly.relayout(CHART_ID, {
      'title.text': spxCode + ' (лог) и ' + nm,
      'yaxis2.title.text': st.code + (st.z ? ' (z-score)' : '')
    });

    document.getElementById('lagVal').textContent = String(st.lag);
  }

  sel.addEventListener('change', function(){ applyState(currentState()); });
  document.getElementById('normChk').addEventListener('change', function(){ applyState(currentState()); });
  document.getElementById('lagRange').addEventListener('input', function(){ applyState(currentState()); });

  applyState(currentState());
})();
""")

    post_js = js_tpl.substitute(
        div_id=div_id,
        macro_payload_json=json.dumps(macro_payload),
        default_code=default_code,
        spx_code=spx_code,
    )

    fig.write_html(
        path,
        include_plotlyjs="cdn",
        full_html=True,
        div_id=div_id,
        post_script=post_js,
    )
    print(f"[OK] Интерактивный отчёт сохранён: {path}")
    return path


def main():
    # данные
    spx_df, spx_code = load_spx_from_db()
    macro_payload, default_code = load_macro_series_payload()

    # базовый макро для старта (без лагов, raw)
    m = macro_payload[default_code]
    fig = make_base_figure(
        spx_df, spx_code, default_code,
        macro_ts=m["ts"], macro_y=m["raw"],
        title_suffix=""
    )

    # сохранить
    save_html(fig, macro_payload, default_code, spx_code)


if __name__ == "__main__":
    main()
