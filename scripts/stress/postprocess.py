"""Summarize persisted native evidence after the campaign has stopped."""

import json
import subprocess
from pathlib import Path
from controller import ROOT


def run(command, path):
    r = subprocess.run(command, text=True, capture_output=True, check=True)
    path.write_text(r.stdout)


event_dir = ROOT / "results/live-event-logs"
apps = json.loads(
    (ROOT / "results/live-applications.jsonl").read_text().splitlines()[-1]
)["applications"]
for app in apps:
    if app["name"].startswith("stress-online"):
        run(
            [
                "python3",
                "scripts/spark_event_report.py",
                str(event_dir),
                "--application",
                app["name"],
            ],
            ROOT / "results/online-spark-metrics.json",
        )
    elif app["name"].startswith("stress-offline-"):
        case = app["name"].removeprefix("stress-offline-")
        run(
            [
                "python3",
                "scripts/spark_event_report.py",
                str(event_dir),
                "--application",
                app["name"],
            ],
            ROOT / ("results/" + case + "-spark-metrics.json"),
        )
progress = ROOT / "results/online-lake/observability/progress"
run(
    ["python3", "scripts/progress_report.py", str(progress)],
    ROOT / "results/online-progress-summary.jsonl",
)
queries = []
for path in (ROOT / "logs").glob("deel-stress-cli-*.log"):
    for line in path.read_text().splitlines():
        if not line.startswith("STRESS "):
            continue
        r = json.loads(line[7:])
        if "query_seconds" in r:
            queries.append(
                dict(
                    name=r["name"],
                    query_seconds=r["query_seconds"],
                    metrics={k: v["seconds"] for k, v in r["metrics"].items()},
                )
            )
(ROOT / "results/query-durations.json").write_text(json.dumps(queries, indent=2) + "\n")
subprocess.run(["python3", str(ROOT / "analyze.py")], check=True)
