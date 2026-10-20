"""Offline summary of actual observations; no production access."""

import datetime as dt
import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def records(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def mib(value):
    number, unit = re.match(r"([0-9.]+)\s*([A-Za-z]+)", value).groups()
    return (
        float(number)
        * {
            "B": 1 / 1024**2,
            "kB": 1 / 1024,
            "KiB": 1 / 1024,
            "MB": 1000000 / 1024**2,
            "MiB": 1,
            "GB": 1000000000 / 1024**2,
            "GiB": 1024,
        }[unit]
    )


resources = defaultdict(list)
for row in records(ROOT / "results/resources.jsonl"):
    resources[row["Name"]].append(row)
resource_summary = {
    name: dict(
        samples=len(rows),
        max_cpu_percent=max(float(r["CPUPerc"].strip("%")) for r in rows),
        max_sampled_memory_mib=max(
            mib(r["MemUsage"].split("/")[0].strip()) for r in rows
        ),
    )
    for name, rows in resources.items()
}

offline = []
for path in sorted((ROOT / "logs").glob("deel-stress-*.log")):
    if not any(s in path.name for s in ("scale-", "hot-")):
        continue
    rows = []
    for line in path.read_text().splitlines():
        if line.startswith("STRESS "):
            try:
                rows.append(json.loads(line[7:]))
            except ValueError:
                pass
    summaries = [r for r in rows if r.get("correctness") == "passed"]
    for report in summaries:
        report = dict(report)
        report["phases"] = [r for r in rows if "batch_id" in r]
        report["resources"] = resource_summary.get("deel-stress-" + report["name"])
        offline.append(report)

out = dict(
    offline=offline,
    resources=resource_summary,
    producer_runs=records(ROOT / "results/producer-runs.jsonl"),
    bronze_levels=records(ROOT / "results/bronze-load-levels.jsonl"),
    rate_levels=records(ROOT / "results/rate-levels.jsonl"),
    drains=records(ROOT / "results/drains.jsonl"),
    recovery=records(ROOT / "results/recovery.jsonl"),
    reconciliation=records(ROOT / "results/reconciliation.jsonl"),
    query_concurrency=records(ROOT / "results/query-concurrency.jsonl"),
)
(ROOT / "results/summary.json").write_text(
    json.dumps(out, indent=2, default=str) + "\n"
)
for r in offline:
    phases = {p["phase"]: p for p in r["phases"]}
    print(
        r["name"],
        r["events"],
        round(r["initial_seconds"], 2),
        round(r["update_seconds"], 2),
        "silver",
        round(phases["initial"]["silver_merge_seconds"], 2),
        "gold",
        round(phases["initial"]["gold_seconds"], 2),
        "resources",
        r["resources"],
    )
