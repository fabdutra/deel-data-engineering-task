"""Decode business data only after the immutable Kafka envelope is persisted."""
from pyspark.sql import Window, functions as F, types as T
from app.schemas import ALL_STATUSES, TABLES


def decode(raw, table):
    pk, ddl = TABLES[table]
    row_schema = T._parse_datatype_string(ddl)
    event_schema = T.StructType([
        T.StructField("before", row_schema), T.StructField("after", row_schema),
        T.StructField("op", T.StringType()), T.StructField("ts_ms", T.LongType()),
        T.StructField("source", T.StructType([
            T.StructField("lsn", T.LongType()), T.StructField("ts_ms", T.LongType()),
        ])),
    ])
    # Kafka null-value tombstones follow Debezium delete events. Do not apply twice.
    data = raw.filter((F.col("topic") == f"finance_db.operations.{table}") & F.col("value").isNotNull())
    data = (data.withColumn("event", F.from_json(F.col("value").cast("string"), event_schema))
            .withColumn("row", F.when(F.col("event.op") == "d", F.col("event.before")).otherwise(F.col("event.after"))))
    key_id = F.get_json_object(F.col("key").cast("string"), f"$.{pk}").cast("long")
    data = data.withColumn("_key_id", key_id).withColumn("_pk", F.coalesce(F.col(f"row.{pk}"), key_id))
    invalid = (F.col("event.op").isNull() | ~F.col("event.op").isin("r", "c", "u", "d")
               | F.col("event.source.lsn").isNull() | F.col("_pk").isNull()
               | ((F.col("event.op") != "d") & F.col("event.after").isNull())
               | (F.col("_key_id").isNotNull() & F.col(f"row.{pk}").isNotNull()
                  & (F.col("_key_id") != F.col(f"row.{pk}"))))
    required = {
        "customers": ["customer_name"],
        "products": ["product_name", "unity_price"],
        "orders": ["customer_id", "status"],
        "order_items": ["order_id", "product_id", "quanity"],
    }
    for field in required[table]:
        invalid = invalid | ((F.col("event.op") != "d") & F.col(f"row.{field}").isNull())
    if table == "orders":
        invalid = invalid | ((F.col("event.op") != "d") & ~F.col("row.status").isin(*ALL_STATUSES))
    if table == "order_items":
        invalid = invalid | ((F.col("event.op") != "d") & (F.col("row.quanity") < 0))
    if table == "products":
        price = F.col("row.unity_price").cast("decimal(20,4)")
        invalid = invalid | ((F.col("event.op") != "d") & (price.isNull() | (price < 0)))
    data = data.withColumn("_invalid", F.coalesce(invalid, F.lit(True)))
    rejected = data.filter("_invalid").select(*raw.columns).withColumn("reason", F.lit(f"Invalid CDC envelope or business fields: {table}"))
    valid = data.filter("NOT _invalid")
    columns = []
    for field in row_schema.fields:
        col = F.col(f"row.{field.name}")
        name = field.name
        if name == pk:
            col = F.col("_pk")
        elif name in ("order_date", "delivery_date"):
            col = F.date_add(F.lit("1970-01-01").cast("date"), col)
        elif name == "unity_price":
            col = col.cast("decimal(20,4)")
        elif name == "quanity":
            name = "quantity"
        columns.append(col.alias(name))
    decoded = valid.select(
        *columns,
        (F.col("event.op") == "d").alias("_deleted"),
        F.col("event.source.lsn").alias("_lsn"),
        F.col("offset").alias("_offset"),
        F.col("partition").alias("_partition"),
        F.col("event.source.ts_ms").alias("_source_ts_ms"),
        "ingested_at",
    )
    # Preserve all touched old/new parent IDs, including item reparenting.
    if table == "orders":
        affected = valid.select(F.col("_pk").alias("order_id"))
    elif table == "order_items":
        affected = (valid.select(F.col("event.before.order_id").alias("order_id"))
                    .union(valid.select(F.col("event.after.order_id").alias("order_id"))))
    else:
        affected = None
    return decoded, rejected, affected


def latest(data, pk):
    window = Window.partitionBy(pk).orderBy(F.col("_lsn").desc(), F.col("_offset").desc())
    return data.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")
