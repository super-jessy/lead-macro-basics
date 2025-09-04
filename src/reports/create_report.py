# src/reports/create_report.py
from __future__ import annotations

import os, json
from datetime import datetime, timezone

from src.reports.loaders import (
    load_spx_from_db,
    load_macro_series_payload,
    load_price_payload,
)
from src.reports.analytics import build_heatmap_matrix, build_ic_payload
from src.reports.charts import build_timeseries_fig, build_heatmap_fig, build_ic_fig
from src.reports.template import PAGE_TEMPLATE


def main():
    # ---- данные (цены только с 2016-01-01) ----
    spx_df, spx_code = load_spx_from_db(start="2016-01-01")
    macro_payload, default_code = load_macro_series_payload()
    price_payload, price_default_code = load_price_payload(start="2016-01-01")

    # ---- фигуры (в дикты) ----
    ts_fig = build_timeseries_fig(spx_df, spx_code, default_code, macro_payload, use_z=False, lag=0)
    ts_fig_spec = json.loads(ts_fig.to_json())

    corr_mat = build_heatmap_matrix(lag_min=-12, lag_max=12, min_obs=24)
    hm_fig = build_heatmap_fig(corr_mat)
    hm_fig_spec = json.loads(hm_fig.to_json())

    ic_payload = build_ic_payload(lag_min=-12, lag_max=12, min_obs=24)
    ic_first_fig = build_ic_fig(default_code, ic_payload)
    ic_fig_spec = json.loads(ic_first_fig.to_json())

    # ---- сборка html ----
    os.makedirs("output", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join("output", f"lead_macro_platform_{ts}.html")

    html = PAGE_TEMPLATE.substitute(
        spx_code=spx_code,
        macro_payload_json=json.dumps(macro_payload),
        default_code=default_code,
        ts_fig_json=json.dumps(ts_fig_spec),
        hm_fig_json=json.dumps(hm_fig_spec),
        ic_payload_json=json.dumps(ic_payload),
        ic_fig_json=json.dumps(ic_fig_spec),
        markets_payload_json=json.dumps(price_payload),   # теперь используем в Timeseries
        market_default_code=price_default_code,
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Платформа сохранена: {path}")


if __name__ == "__main__":
    main()
