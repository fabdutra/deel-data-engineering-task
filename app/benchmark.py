"""Bounded-memory synthetic materialization benchmark, isolated from the live lake."""
import dataclasses
import datetime as dt
import json
import tempfile
import time
from pathlib import Path
from pyspark.sql import functions as F
from app.materialize import initialize, materialize_batch
from app.schemas import TABLES
from app.storage import read_delta


def envelope(data, table, lsn, offset_base=0):
    pk = TABLES[table][0]
    payload = F.struct(F.lit("c" if lsn == 1 else "u").alias("op"),
                       F.struct(*[F.col(c) for c in data.columns]).alias("after"),
                       F.struct(F.lit(lsn).alias("lsn"),
                                (F.unix_timestamp() * 1000).alias("ts_ms")).alias("source"))
    return data.select(
        F.to_json(F.struct(F.col(pk))).cast("binary").alias("key"),
        F.to_json(payload).cast("binary").alias("value"),
        F.lit(f"finance_db.operations.{table}").alias("topic"),
        F.lit(0).alias("partition"), (F.col(pk) + offset_base).cast("long").alias("offset"),
        F.current_timestamp().alias("kafka_timestamp"), F.current_timestamp().alias("ingested_at"),
    )


def run(spark, settings, orders, items_per_order):
    if orders < 1 or items_per_order < 1:
        raise ValueError("Benchmark sizes must be positive")
    base = Path(settings.path("benchmarks"))
    base.mkdir(parents=True, exist_ok=True)
    location = tempfile.mkdtemp(prefix="run-", dir=base)
    isolated = dataclasses.replace(settings, lake_root=location)
    started = time.monotonic()
    initialize(spark, isolated)
    init_seconds = time.monotonic() - started
    customers = spark.range(1, 101).selectExpr("id AS customer_id", "concat('Customer ', id) AS customer_name", "true AS is_active")
    products = spark.range(1, 101).selectExpr("id AS product_id", "concat('Product ', id) AS product_name", "'12.34' AS unity_price")
    order_rows = spark.range(1, orders + 1).selectExpr("id AS order_id", "(id % 100) + 1 AS customer_id", "'PENDING' AS status", "20000 AS order_date", "CAST(20010 + (id % 30) AS INT) AS delivery_date")
    item_rows = spark.range(1, orders * items_per_order + 1).selectExpr(
        "id AS order_item_id", f"CAST(FLOOR((id - 1) / {items_per_order}) + 1 AS BIGINT) AS order_id",
        "(id % 100) + 1 AS product_id", "CAST((id % 9) + 1 AS INT) AS quanity")
    raw = envelope(customers, "customers", 1).unionByName(envelope(products, "products", 1)).unionByName(envelope(order_rows, "orders", 1)).unionByName(envelope(item_rows, "order_items", 1))
    start = time.monotonic()
    raw.write.mode("errorifexists").parquet(isolated.path("bronze/events"))
    bronze_seconds = time.monotonic() - start
    persisted = spark.read.parquet(isolated.path("bronze/events"))
    start = time.monotonic()
    materialize_batch(spark, isolated, persisted, 0)
    initial_seconds = time.monotonic() - start
    updates = envelope(order_rows.filter("order_id % 10 = 0").withColumn("status", F.lit("COMPLETED")), "orders", 2, orders)
    start = time.monotonic()
    materialize_batch(spark, isolated, updates, 1)
    update_seconds = time.monotonic() - start
    facts = read_delta(spark, isolated.path("gold/fact_order"))
    completed = facts.filter("status = 'COMPLETED' AND NOT _deleted").count()
    assert facts.count() == orders and completed == orders // 10
    files = list(Path(location).rglob("*.parquet"))
    row_count = 200 + orders * (1 + items_per_order)
    report = {
        "kind": "synthetic_materialization_not_kafka_end_to_end",
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "spark_version": spark.version, "master": spark.sparkContext.master,
        "shuffle_partitions": settings.shuffle_partitions,
        "orders": orders, "items_per_order": items_per_order, "initial_events": row_count,
        "initialization_seconds": round(init_seconds, 3),
        "bronze_write_seconds": round(bronze_seconds, 3),
        "initial_materialization_seconds": round(initial_seconds, 3),
        "initial_events_per_second": round(row_count / initial_seconds, 2),
        "order_updates": orders // 10,
        "update_materialization_seconds": round(update_seconds, 3),
        "parquet_files_including_bronze_and_old_versions": len(files),
        "parquet_bytes_including_bronze_and_old_versions": sum(p.stat().st_size for p in files),
        "correctness": "passed", "lake_root": location,
    }
    target = Path(location) / "report.json"
    target.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)
