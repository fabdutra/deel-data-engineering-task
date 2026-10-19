"""Summarize persisted Structured Streaming progress without Spark dependencies."""
import argparse
import json
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("path", type=Path)
args = parser.parse_args()
queries = defaultdict(list)
for path in args.path.glob("*.json"):
    item = json.loads(path.read_text())
    if item.get("numInputRows", 0) > 0:
        queries[item["name"]].append(item)


def percentile(values, quantile):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * quantile))]


for name, batches in queries.items():
    durations = [b.get("durationMs", {}).get("triggerExecution", 0) for b in batches]
    print(json.dumps({"query": name, "nonempty_batches": len(batches),
                      "reported_input_rows": sum(b["numInputRows"] for b in batches),
                      "batch_duration_ms_p50": percentile(durations, .50),
                      "batch_duration_ms_p95": percentile(durations, .95),
                      "batch_duration_ms_max": max(durations)}, sort_keys=True))
