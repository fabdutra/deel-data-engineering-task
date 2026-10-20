import argparse
import dataclasses
import json
import logging
import sys
import time
from pathlib import Path
from pyspark.sql import functions as F
from app.config import Settings
from app.session import session
from app.benchmark import envelope
from app.materialize import initialize
from app.storage import read_delta, exists
from app.metrics import load_publication, register_views, result, METRICS
import instrument

parser = argparse.ArgumentParser()
parser.add_argument(
    "mode",
    choices=["offline", "online", "query", "bronze", "bronze-check", "online-check"],
)
parser.add_argument("--name", default="run")
parser.add_argument("--orders", type=int, default=100000)
parser.add_argument("--items", type=int, default=5)
parser.add_argument("--hot", action="store_true")
parser.add_argument("--resume", action="store_true")
parser.add_argument("--expected-count", type=int)
args = parser.parse_args()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
    stream=sys.stderr,
)
logging.getLogger("py4j").setLevel(logging.WARNING)
settings = Settings()
spark = session(settings, "stress-" + args.mode + "-" + args.name)


def offline():
    local = dataclasses.replace(
        settings, lake_root=settings.path("offline/" + args.name)
    )
    start = time.monotonic()
    initialize(spark, local)
    init_time = time.monotonic() - start
    n = args.orders
    m = args.items
    customers = spark.range(1, 101).selectExpr(
        "id AS customer_id",
        "concat('Customer ',id) AS customer_name",
        "true AS is_active",
    )
    products = spark.range(1, 101).selectExpr(
        "id AS product_id",
        "concat('Product ',id) AS product_name",
        "'12.34' AS unity_price",
    )
    orders = spark.range(1, n + 1).selectExpr(
        "id AS order_id",
        "(id % 100)+1 AS customer_id",
        "'PENDING' AS status",
        "20000 AS order_date",
        "CAST(20010+(id%30) AS INT) AS delivery_date",
    )
    expression = (
        "1 AS order_id"
        if args.hot
        else f"CAST(FLOOR((id-1)/{m})+1 AS BIGINT) AS order_id"
    )
    items = spark.range(1, n * m + 1).selectExpr(
        "id AS order_item_id",
        expression,
        "(id%100)+1 AS product_id",
        "CAST((id%9)+1 AS INT) AS quanity",
    )
    path = local.path("bronze/events")
    bronze = 0
    if not exists(spark, path):
        raw = (
            envelope(customers, "customers", 1)
            .unionByName(envelope(products, "products", 1))
            .unionByName(envelope(orders, "orders", 1))
            .unionByName(envelope(items, "order_items", 1))
        )
        start = time.monotonic()
        raw.write.parquet(path)
        bronze = time.monotonic() - start
    raw = spark.read.parquet(path)
    start = time.monotonic()
    instrument.batch(spark, local, raw, 0, phase="initial")
    initial = time.monotonic() - start
    selected = orders.filter("order_id=1" if args.hot else "order_id%10=0")
    updates = envelope(
        selected.withColumn("status", F.lit("COMPLETED")), "orders", 2, n
    )
    start = time.monotonic()
    instrument.batch(spark, local, updates, 1, phase="update")
    update = time.monotonic() - start
    facts = read_delta(spark, local.path("gold/fact_order"))
    itemfacts = read_delta(spark, local.path("gold/fact_order_item"))
    expected = 1 if args.hot else n // 10
    assert facts.count() == n
    assert facts.filter("status='COMPLETED' AND NOT _deleted").count() == expected
    assert itemfacts.count() == n * m
    expected_items = n * m if args.hot else expected * m
    assert (
        itemfacts.filter("status='COMPLETED' AND NOT _deleted").count()
        == expected_items
    )
    assert itemfacts.filter("_deleted").count() == 0
    # Real metric SQL, timed on a fully published snapshot.
    register_views(spark, local, load_publication(spark, local))
    queries = {}
    for metric in METRICS:
        start = time.monotonic()
        rows = result(spark, metric).collect()
        queries[metric] = dict(seconds=time.monotonic() - start, rows=len(rows))
    report = dict(
        name=args.name,
        orders=n,
        items=n * m,
        events=200 + n + n * m,
        hot=args.hot,
        init_seconds=init_time,
        bronze_seconds=bronze,
        initial_seconds=initial,
        update_seconds=update,
        initial_events_per_second=(200 + n + n * m) / initial,
        queries=queries,
        correctness="passed",
        lake=local.lake_root,
    )
    instrument.emit(local.path("telemetry/report.jsonl"), report)


def query():
    start = time.monotonic()
    publication = load_publication(spark, settings)
    register_views(spark, settings, publication)
    metrics = {}
    for metric in METRICS:
        qstart = time.monotonic()
        rows = result(spark, metric).collect()
        metrics[metric] = dict(
            seconds=time.monotonic() - qstart, rows=[r.asDict() for r in rows]
        )
    # Independent aggregate facts for reconciliation after the source is quiet.
    ordercounts = (
        spark.sql(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed FROM fact_order"
        )
        .first()
        .asDict()
    )
    itemcounts = (
        spark.sql(
            "SELECT COUNT(*) AS total, SUM(quantity) AS units FROM fact_order_item"
        )
        .first()
        .asDict()
    )
    instrument.emit(
        settings.path("telemetry/queries.jsonl"),
        dict(
            name=args.name,
            query_seconds=time.monotonic() - start,
            publication=publication.asDict(recursive=True),
            metrics=metrics,
            ordercounts=ordercounts,
            itemcounts=itemcounts,
        ),
    )


try:
    if args.mode == "offline":
        offline()
    elif args.mode == "query":
        query()
    elif args.mode == "bronze-check":
        data = spark.read.parquet("/lake/bronze-only/bronze/events")
        count = data.count()
        unique = data.select("topic", "partition", "offset").distinct().count()
        assert count == 700000 and unique == count
        instrument.emit(
            "/lake/bronze-only/verification.jsonl",
            dict(rows=count, unique_coordinates=unique, correctness="passed"),
        )
    elif args.mode == "online-check":
        data = spark.read.parquet("/lake/online/bronze/events")
        count = data.count()
        unique = data.select("topic", "partition", "offset").distinct().count()
        assert count == args.expected_count and unique == count
        instrument.emit(
            "/lake/online/telemetry/integrity.jsonl",
            dict(
                rows=count,
                unique_coordinates=unique,
                expected=args.expected_count,
                correctness="passed",
            ),
        )
    elif args.mode == "bronze":
        import app.bronze as bronze
        from app.observability import ProgressRecorder

        bronze.TOPICS = "stress.raw"
        cfg = dataclasses.replace(settings, lake_root="/lake/bronze-only")
        spark.streams.addListener(ProgressRecorder(cfg.lake_root))
        stream = bronze.start_bronze(spark, cfg)
        stream.awaitTermination()
    else:
        import app.materialize as mat

        mat.materialize_batch = lambda s, c, r, b: instrument.batch(
            s, c, r, b, online=True
        )
        from app.pipeline import run

        run(spark, settings)
finally:
    spark.stop()
