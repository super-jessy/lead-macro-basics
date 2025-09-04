# src/reports/create_report.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from string import Template
from typing import Dict, List

from src.reports.loaders import load_spx_payload, load_macro_series_payload
from src.reports.analytics import build_heatmap_matrix, build_ic_payload
from src.reports.plots import build_heatmap_fig, build_ic_fig


# ---- безопасное чтение шаблона: экранируем все $ кроме наших ----
KNOWN_KEYS: List[str] = [
    "spx_code",
    "spx_payload_json",
    "macro_payload_json",
    "default_code",
    "hm_fig_json",
    "ic_payload_json",
    "ic_fig_json",
]

def read_template_safely() -> Template:
    here = os.path.dirname(__file__)
    path = os.path.join(here, "templates", "platform.html")
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()

    # 1) временно метим наши плейсхолдеры, чтобы их не трогать
    for k in KNOWN_KEYS:
        txt = txt.replace(f"${{{k}}}", f"@@__{k}__@@")

    # 2) экранируем вообще все доллары, чтобы Template их не видел
    txt = txt.replace("$", "$$")

    # 3) возвращаем наши плейсхолдеры обратно к виду ${key}
    for k in KNOWN_KEYS:
        txt = txt.replace(f"@@__{k}__@@", f"${{{k}}}")

    return Template(txt)


def main():
    # --- данные ---
    spx_payload, spx_code = load_spx_payload()
    macro_payload, default_code = load_macro_series_payload()

    # --- Heatmap & IC ---
    corr_mat = build_heatmap_matrix(lag_min=-12, lag_max=12, min_obs=24)
    hm_fig_spec: Dict = json.loads(build_heatmap_fig(corr_mat).to_json())

    ic_payload = build_ic_payload(lag_min=-12, lag_max=12, min_obs=24)
    ic_fig_spec: Dict = json.loads(build_ic_fig(default_code, ic_payload).to_json())

    # --- сборка HTML ---
    os.makedirs("output", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("output", f"lead_macro_platform_{ts}.html")

    tpl = read_template_safely()
    html = tpl.substitute(
        spx_code=spx_code,
        spx_payload_json=json.dumps(spx_payload),
        macro_payload_json=json.dumps(macro_payload),
        default_code=default_code,
        hm_fig_json=json.dumps(hm_fig_spec),
        ic_payload_json=json.dumps(ic_payload),
        ic_fig_json=json.dumps(ic_fig_spec),
    )

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Платформа сохранена: {out_path}")


if __name__ == "__main__":
    main()
