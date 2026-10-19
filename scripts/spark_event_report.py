"""Read Spark JSON event logs and summarize benchmark task costs (no Spark needed)."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("path", type=Path)
parser.add_argument("--application", default="acme-benchmark")
args = parser.parse_args()


def quantile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))] if ordered else 0


for path in ([args.path] if args.path.is_file() else sorted(args.path.iterdir())):
    if not path.is_file() or path.name.startswith('.'):
        continue
    totals = defaultdict(int)
    stages = defaultdict(lambda: defaultdict(int))
    durations = []
    application = None
    app_id = None
    with path.open() as source:
        for line in source:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = event.get("Event")
            if kind == "SparkListenerApplicationStart":
                application = event.get("App Name")
                app_id = event.get("App ID")
                if application != args.application:
                    break
            if kind != "SparkListenerTaskEnd":
                continue
            task = event.get("Task Metrics", {})
            values = {
                "task_attempts": 1,
                "executor_run_ms": task.get("Executor Run Time", 0),
                "executor_cpu_ns": task.get("Executor CPU Time", 0),
                "jvm_gc_ms": task.get("JVM GC Time", 0),
                "memory_bytes_spilled": task.get("Memory Bytes Spilled", 0),
                "disk_bytes_spilled": task.get("Disk Bytes Spilled", 0),
                "input_bytes": task.get("Input Metrics", {}).get("Bytes Read", 0),
                "shuffle_read_bytes": sum(task.get("Shuffle Read Metrics", {}).get(k, 0) for k in ("Remote Bytes Read", "Local Bytes Read")),
                "shuffle_write_bytes": task.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0),
                "failed_task_attempts": int(event.get("Task Info", {}).get("Failed", False)),
            }
            durations.append(values["executor_run_ms"])
            for name, value in values.items():
                totals[name] += value
                stages[event["Stage ID"]][name] += value
    if application == args.application:
        top = sorted(stages.items(), key=lambda item: item[1]["executor_run_ms"], reverse=True)[:10]
        print(json.dumps({"application": application, "application_id": app_id,
                          "event_log": path.name, "totals": dict(totals),
                          "task_run_ms_p50": quantile(durations, .5), "task_run_ms_p95": quantile(durations, .95),
                          "task_run_ms_max": max(durations, default=0),
                          "top_stages_by_total_executor_time": [{"stage_id": i, **dict(values)} for i, values in top]}, indent=2))
