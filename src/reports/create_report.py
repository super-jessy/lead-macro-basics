import os
import json
from datetime import datetime, timezone
from string import Template

# наши модульные функции
from src.reports.loaders import load_spx_from_db, load_macro_series_payload
from src.reports.analytics import build_heatmap_matrix, build_ic_payload
from src.reports.plots import build_timeseries_fig, build_heatmap_fig, build_ic_fig


def _read_template() -> Template:
    """Читает HTML-шаблон платформы и возвращает как Template."""
    tpl_path = os.path.join(os.path.dirname(__file__), "templates", "platform.html")
    with open(tpl_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def main():
    # 1) Данные из БД
    spx_df, spx_code = load_spx_from_db()
    macro_payload, default_code = load_macro_series_payload()

    # 2) Фигуры -> dict-спеки (НЕ строки), чтобы потом безопасно сериализовать
    ts_fig = build_timeseries_fig(
        spx_df=spx_df,
        spx_code=spx_code,
        code=default_code,
        macro_payload=macro_payload,
        use_z=False,
        lag=0,
    )
    ts_fig_spec = json.loads(ts_fig.to_json())

    corr_mat = build_heatmap_matrix(lag_min=-12, lag_max=12, min_obs=24)
    hm_fig = build_heatmap_fig(corr_mat)
    hm_fig_spec = json.loads(hm_fig.to_json())

    ic_payload = build_ic_payload(lag_min=-12, lag_max=12, min_obs=24)
    ic_first_fig = build_ic_fig(default_code, ic_payload)
    ic_fig_spec = json.loads(ic_first_fig.to_json())

    # 3) Сборка HTML из шаблона
    tpl = _read_template()
    html = tpl.substitute(
        # ВАЖНО: всё, что идёт в <script type="application/json"> — должно быть валидным JSON
        spx_code_json=json.dumps(spx_code),
        default_code_json=json.dumps(default_code),
        macro_payload_json=json.dumps(macro_payload),
        ts_fig_json=json.dumps(ts_fig_spec),
        hm_fig_json=json.dumps(hm_fig_spec),
        ic_payload_json=json.dumps(ic_payload),
        ic_fig_json=json.dumps(ic_fig_spec),
        page_title="DELTA TERMINAL - MVP",
        h1_title="DELTA TERMINAL - MVP",
    )

    # 4) Сохраняем
    os.makedirs("output", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join("output", f"lead_macro_platform_{ts}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Платформа сохранена: {path}")


if __name__ == "__main__":
    main()
