# src/reports/template.py
from string import Template

PAGE_TEMPLATE = Template(r"""<!doctype html>
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

  <!-- Timeseries -->
  <div id="panel-timeseries" class="panel active">
    <div class="controls">
      <label>Инструмент:
        <select id="mktSelect" style="margin-left:6px; padding:2px 6px;"></select>
      </label>
      <label style="margin-left:12px;">
        <input type="checkbox" id="candleChk" style="transform: translateY(2px); margin-right:6px;">
        Свечи (candlestick)
      </label>

      <div style="width:1px; height:20px; background:#ddd; margin:0 6px;"></div>

      <label>Индикатор:
        <select id="macroSelect" style="margin-left:6px; padding:2px 6px;"></select>
      </label>
      <label>Нормировка:
        <input type="checkbox" id="normChk" style="transform: translateY(2px); margin:0 6px;"> z-score
      </label>
      <label>Лаг (мес): <span id="lagVal">0</span>
        <input type="range" id="lagRange" min="-12" max="12" step="1" value="0" style="vertical-align: middle; margin-left:8px;">
      </label>
    </div>
    <div id="fig_timeseries"></div>
  </div>

  <!-- Heatmap -->
  <div id="panel-heatmap" class="panel">
    <div id="fig_heatmap"></div>
  </div>

  <!-- IC -->
  <div id="panel-ic" class="panel">
    <div class="controls">
      <label for="icSelect">Индикатор:</label>
      <select id="icSelect"></select>
    </div>
    <div id="fig_ic"></div>
  </div>

  <!-- Models (placeholder) -->
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
    // ======= данные из Python =======
    const spxCode      = "${spx_code}";
    const macroData    = ${macro_payload_json};
    const defaultCode  = "${default_code}";
    const tsFigureSpec = ${ts_fig_json};
    const hmFigureSpec = ${hm_fig_json};
    const icPayload    = ${ic_payload_json};
    const icFigureSpec = ${ic_fig_json};

    // цены для всех инструментов (с 2016-01-01)
    const priceData    = ${markets_payload_json};
    const defaultMkt   = "${market_default_code}";

    // ======= табы =======
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

    // ======= Timeseries =======
    // инициализируем селект инструмента
    const selM = document.getElementById('mktSelect');
    const mktCodes = Object.keys(priceData).sort();
    mktCodes.forEach(code => {
      const opt = document.createElement('option');
      opt.value = code; opt.textContent = code;
      if (code === defaultMkt) opt.selected = true;
      selM.appendChild(opt);
    });

    // инициализируем селект макро
    const selMacro = document.getElementById('macroSelect');
    Object.keys(macroData).sort().forEach(code => {
      const opt = document.createElement('option');
      opt.value = code; opt.textContent = code;
      if (code === defaultCode) opt.selected = true;
      selMacro.appendChild(opt);
    });

    // стартовая отрисовка
    Plotly.newPlot('fig_timeseries', tsFigureSpec.data, tsFigureSpec.layout, {responsive:true});
    applyState(currentState()); // чтобы подхватить выбранный инструмент/режим

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
      const mkt = selM.value;
      const candle = document.getElementById('candleChk').checked;

      const code = selMacro.value;
      const z = document.getElementById('normChk').checked;
      const lag = parseInt(document.getElementById('lagRange').value, 10) || 0;
      return { mkt, candle, code, z, lag };
    }

    function legendNameMacro(st) {
      const suffix = [];
      if (st.z) suffix.push('z');
      if (st.lag) suffix.push('lag ' + (st.lag>0? '+'+st.lag: st.lag) + 'm');
      return st.code + (suffix.length? ' ('+suffix.join(', ')+')' : '');
    }

    function applyState(st) {
      // --- инструмент (trace 0) ---
      const rec = priceData[st.mkt];
      if (!rec) return;

      if (st.candle) {
        const candle = {
          type: "candlestick",
          x: rec.ts, open: rec.open, high: rec.high, low: rec.low, close: rec.close,
          name: st.mkt, yaxis: "y"
        };
        // Полная замена первого трейса
        Plotly.deleteTraces('fig_timeseries', 0);
        Plotly.addTraces('fig_timeseries', [candle], 0);
      } else {
        // линия по close
        Plotly.restyle('fig_timeseries', {
          x: [rec.ts],
          y: [rec.close],
          name: [st.mkt],
          type: ["scatter"],
          mode: ["lines"],
          line: [{width: 2.0}],
          yaxis: ["y"]
        }, [0]);
      }

      // --- макро (trace 1) ---
      const m = macroData[st.code];
      const y = st.z ? m.z : m.raw;
      const x = withLag(m.ts, st.lag);
      const nm = legendNameMacro(st);
      Plotly.restyle('fig_timeseries', { x:[x], y:[y], name:[nm], 'yaxis':['y2'] }, [1]);

      // --- релейаут ---
      Plotly.relayout('fig_timeseries', {
        'title.text': st.mkt + ' и ' + nm,
        'yaxis.title.text': st.mkt,
        'yaxis2.title.text': st.code + (st.z ? ' (z-score)' : '')
      });

      document.getElementById('lagVal').textContent = String(st.lag);
    }

    // события
    selM.addEventListener('change', ()=>applyState(currentState()));
    document.getElementById('candleChk').addEventListener('change', ()=>applyState(currentState()));
    selMacro.addEventListener('change', ()=>{ const st=currentState(); applyState(st); updateIC(st.code); });
    document.getElementById('normChk').addEventListener('change', ()=>applyState(currentState()));
    document.getElementById('lagRange').addEventListener('input', ()=>applyState(currentState()));

    // ======= IC =======
    const icSelect = document.getElementById("icSelect");
    Object.keys(macroData).sort().forEach(code => {
      const opt = document.createElement("option");
      opt.value = code;
      opt.textContent = code;
      icSelect.appendChild(opt);
    });
    icSelect.value = defaultCode;
    icSelect.addEventListener("change", () => updateIC(icSelect.value));

    function updateIC(code){
      const item = icPayload[code];
      if (!item) return;
      if (!window.__icRendered) return;
      try {
        Plotly.react('fig_ic',
          [{ x: item.lags, y: item.ic, mode: 'lines+markers', name: 'IC' }],
          {
            ...icFigureSpec.layout,
            title: {text: 'Information Coefficient (next-month return) — ' + code}
          },
          {responsive:true}
        );
      } catch(e){ showError(e); }
    }

  } catch (e) {
    showError(e);
  }
</script>

</body>
</html>
""")
