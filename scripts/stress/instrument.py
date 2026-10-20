"""Test-only timers around the unchanged production functions."""

import datetime as dt
import json
import threading
import time
from pathlib import Path
from pyspark.sql import functions as F
import app.materialize as mat
import app.gold as gold
from app.storage import merge as original_merge

STATE = threading.local()
ORIGINAL_BATCH = mat.materialize_batch
ORIGINAL_GOLD = mat.refresh_gold
ORIGINAL_PUBLISH = mat.publish


def emit(path, record):
    record["recorded_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as out:
        out.write(json.dumps(record, default=str) + "\n")
        out.flush()
    print("STRESS " + json.dumps(record, default=str), flush=True)


def timed_merge(spark, path, source, keys, versioned=False):
    start = time.monotonic()
    if getattr(STATE, "record", None) is not None:
        print("STRESS_STAGE " + "/".join(path.split("/")[-2:]), flush=True)
    try:
        return original_merge(spark, path, source, keys, versioned)
    finally:
        record = getattr(STATE, "record", None)
        if record is not None:
            label = "/".join(path.split("/")[-2:])
            record["merge_seconds"][label] = (
                record["merge_seconds"].get(label, 0) + time.monotonic() - start
            )


def timed_gold(*args, **kwargs):
    start = time.monotonic()
    try:
        return ORIGINAL_GOLD(*args, **kwargs)
    finally:
        record = getattr(STATE, "record", None)
        if record is not None:
            record["gold_seconds"] = time.monotonic() - start


def timed_publish(spark, settings, batch_id, row_count, start):
    record = getattr(STATE, "record", None)
    if record is not None:
        record["input_events"] = row_count
        if record.get("online"):
            stats_start = time.monotonic()
            raw = STATE.raw
            # Cached by the original materializer at this point; one extra job.
            event = F.col("value").cast("string")
            ts = F.when(
                F.get_json_object(event, "$.op") != "r",
                F.get_json_object(event, "$.source.ts_ms").cast("long"),
            )
            row = (
                raw.select(ts.alias("ts"))
                .agg(
                    F.min("ts").alias("min_ts"),
                    F.max("ts").alias("max_ts"),
                    F.percentile_approx("ts", [0.05, 0.5], 1000).alias("quantiles"),
                )
                .first()
            )
            record["telemetry_job_seconds"] = time.monotonic() - stats_start
            record["source_ts_min_ms"] = row["min_ts"]
            record["source_ts_max_ms"] = row["max_ts"]
            record["source_ts_quantiles"] = row["quantiles"]
    pub_start = time.monotonic()
    result = ORIGINAL_PUBLISH(spark, settings, batch_id, row_count, start)
    if record is not None:
        record["publication_seconds"] = time.monotonic() - pub_start
        now_ms = time.time() * 1000
        quantiles = record.pop("source_ts_quantiles", None)
        if quantiles and quantiles[0] is not None:
            record["source_to_publication_age_p95_seconds"] = (
                now_ms - quantiles[0]
            ) / 1000
            record["source_to_publication_age_p50_seconds"] = (
                now_ms - quantiles[1]
            ) / 1000
    return result


def batch(spark, settings, raw, batch_id, *, phase="online", online=False, fault=None):
    STATE.record = dict(
        batch_id=int(batch_id), phase=phase, online=online, merge_seconds={}
    )
    STATE.raw = raw
    start = time.monotonic()
    try:
        ORIGINAL_BATCH(spark, settings, raw, batch_id, fault)
        STATE.record["status"] = "committed"
    except BaseException as exc:
        STATE.record["status"] = "failed"
        STATE.record["error"] = str(exc)[:1500]
        raise
    finally:
        record = STATE.record
        record["wall_seconds"] = time.monotonic() - start
        record["silver_merge_seconds"] = sum(
            v for k, v in record["merge_seconds"].items() if k.startswith("silver/")
        )
        record["validation_and_other_seconds"] = (
            record["wall_seconds"]
            - record["silver_merge_seconds"]
            - record.get("gold_seconds", 0)
            - record.get("publication_seconds", 0)
            - record.get("telemetry_job_seconds", 0)
        )
        emit(settings.path("telemetry/batches.jsonl"), record)
        STATE.record = None
        STATE.raw = None


mat.merge = timed_merge
gold.merge = timed_merge
mat.refresh_gold = timed_gold
mat.publish = timed_publish
