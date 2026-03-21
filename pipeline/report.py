import os
import logging
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

log = logging.getLogger(__name__)
os.makedirs("reports", exist_ok=True)


def generate_report(validations: dict, anomalies: dict,
                    transforms: dict, loads: dict,
                    duration_sec: float) -> str:

    total_rows   = sum(r.rows_loaded for r in loads.values())
    total_anoms  = sum(r.count for r in anomalies.values())
    scores       = [v.summary()["score"] for v in validations.values()]
    avg_score    = round(sum(scores) / len(scores), 1)
    datasets_ok  = sum(1 for v in validations.values() if v.passed)
    total_steps  = sum(len(t.steps) for t in transforms.values())
    overall      = "PASS" if all(v.passed for v in validations.values()) else "FAIL"

    val_data = {}
    for name, v in validations.items():
        s = v.summary()
        val_data[name] = {
            "score":  s["score"],
            "status": s["status"],
            "checks": v.checks
        }

    env      = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("report.html")

    html = template.render(
        run_at             = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        overall_status     = overall,
        duration_sec       = round(duration_sec, 2),
        total_rows         = total_rows,
        datasets_passed    = datasets_ok,
        datasets_total     = len(loads),
        total_anomalies    = total_anoms,
        avg_quality_score  = avg_score,
        total_lineage_steps= total_steps,
        validations        = val_data,
        anomalies          = anomalies,
        loads              = loads,
        transforms         = transforms,
    )

    path = f"reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"Report saved to {path}")
    return path