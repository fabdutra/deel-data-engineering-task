import datetime as dt
import logging
import time
from pyspark.sql import functions as F, types as T
from app.cdc import decode, latest
from app.gold import initialize_gold, refresh_gold
from app.schemas import BRONZE_SCHEMA, GOLD_TABLES, TABLES
from app.storage import delta_exists, merge, version

LOG = logging.getLogger(__name__)


def initialize(spark, settings):
    empty = spark.createDataFrame([], BRONZE_SCHEMA)
    for table in TABLES:
        path = settings.path(f"silver/{table}")
        if not delta_exists(spark, path):
            data, _, _ = decode(empty, table)
            data.write.format("delta").mode("errorifexists").save(path)
    initialize_gold(spark, settings)


def publish(spark, settings, batch_id, row_count, start):
    versions = {name: version(spark, settings.path(f"gold/{name}")) for name in GOLD_TABLES}
    now = dt.datetime.now(dt.timezone.utc)
    schema = "id int, batch_id long, published_at timestamp, input_events long, materialization_ms long, versions map<string,bigint>"
    manifest = spark.createDataFrame([(1, int(batch_id), now, row_count, int((time.monotonic() - start) * 1000), versions)], schema)
    # A single-table Delta transaction publishes the complete set of versions.
    merge(spark, settings.path("gold/publication"), manifest, ["id"])
    LOG.info("published batch=%d events=%d duration_ms=%d versions=%s", batch_id, row_count,
             int((time.monotonic() - start) * 1000), versions)


def materialize_batch(spark, settings, raw, batch_id, fault=None):
    if raw.isEmpty():
        return
    start = time.monotonic()
    raw = raw.persist()
    changes = {}
    try:
        row_count = raw.count()
        # Validate all four topics before touching current-state tables.
        for table in TABLES:
            data, rejected, _ = decode(raw, table)
            if not rejected.isEmpty():
                merge(spark, settings.path("quarantine/events"), rejected,
                      ["topic", "partition", "offset"])
                raise ValueError(f"Invalid {table} CDC event quarantined; publication and checkpoint withheld")
            changes[table] = latest(data, TABLES[table][0]).persist()
        for table, data in changes.items():
            merge(spark, settings.path(f"silver/{table}"), data, [TABLES[table][0]], versioned=True)
        if fault:
            fault("after_silver")
        refresh_gold(spark, settings, changes)
        if fault:
            fault("before_publication")
        publish(spark, settings, batch_id, row_count, start)
    finally:
        for data in changes.values():
            data.unpersist()
        raw.unpersist()
