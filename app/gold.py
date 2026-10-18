from pyspark.sql import functions as F
from app.storage import live, merge, read_delta


def date_key(column):
    return F.coalesce(F.date_format(column, "yyyyMMdd").cast("int"), F.lit(0))


def order_fact(orders):
    return orders.select(
        "order_id", "customer_id", "status",
        date_key(F.col("order_date")).alias("order_date_key"),
        date_key(F.col("delivery_date")).alias("delivery_date_key"),
        "_deleted", "_source_ts_ms", "ingested_at",
    ).withColumnRenamed("customer_id", "customer_key")


def item_fact(items, orders):
    return items.alias("i").join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "left").select(
        F.col("i.order_item_id"), F.col("i.order_id"),
        F.col("i.product_id").alias("product_key"),
        F.col("o.customer_id").alias("customer_key"),
        F.col("i.quantity"), F.col("o.status"),
        date_key(F.col("o.delivery_date")).alias("delivery_date_key"),
        (F.col("i._deleted") | F.coalesce(F.col("o._deleted"), F.lit(True))).alias("_deleted"),
        F.greatest(F.col("i._source_ts_ms"), F.col("o._source_ts_ms")).alias("_source_ts_ms"),
        F.greatest(F.col("i.ingested_at"), F.col("o.ingested_at")).alias("ingested_at"),
    )


def customer_dimension(data):
    return data.select(F.col("customer_id").alias("customer_key"), "customer_id", "customer_name", "customer_address", "is_active", "_deleted")


def product_dimension(data):
    return data.select(F.col("product_id").alias("product_key"), "product_id", "product_name", "barcode", "unity_price", "is_active", "_deleted")


def date_dimension(orders):
    dates = orders.select(F.col("order_date").alias("date")).union(orders.select(F.col("delivery_date").alias("date"))).distinct()
    return dates.select(date_key(F.col("date")).alias("date_key"), "date",
                        F.year("date").alias("year"), F.month("date").alias("month"), F.dayofmonth("date").alias("day"))


def initialize_gold(spark, settings):
    orders = read_delta(spark, settings.path("silver/orders")).limit(0)
    items = read_delta(spark, settings.path("silver/order_items")).limit(0)
    schemas = {
        "dim_customer": customer_dimension(read_delta(spark, settings.path("silver/customers")).limit(0)),
        "dim_product": product_dimension(read_delta(spark, settings.path("silver/products")).limit(0)),
        "dim_date": date_dimension(orders),
        "fact_order": order_fact(orders),
        "fact_order_item": item_fact(items, orders),
    }
    from app.storage import delta_exists
    for name, data in schemas.items():
        path = settings.path(f"gold/{name}")
        if not delta_exists(spark, path):
            data.write.format("delta").mode("errorifexists").save(path)
    # Unknown date is a dimension member even before a null-dated order arrives.
    unknown = spark.sql("SELECT 0 AS date_key, CAST(NULL AS DATE) AS date, CAST(NULL AS INT) AS year, CAST(NULL AS INT) AS month, CAST(NULL AS INT) AS day")
    merge(spark, settings.path("gold/dim_date"), unknown, ["date_key"])


def refresh_gold(spark, settings, changes):
    orders = read_delta(spark, settings.path("silver/orders"))
    items = read_delta(spark, settings.path("silver/order_items"))
    changed_orders = changes["orders"].select("order_id").distinct()
    changed_items = changes["order_items"].select("order_item_id")
    # Recompute affected children on parent status/date/customer changes. A left
    # join in item_fact retains unresolved children and repairs them on arrival.
    affected_items = changed_items.union(items.join(changed_orders, "order_id", "left_semi").select("order_item_id")).distinct()
    order_rows = orders.join(changed_orders, "order_id", "left_semi")
    item_rows = items.join(affected_items, "order_item_id", "left_semi")
    merge(spark, settings.path("gold/fact_order"), order_fact(order_rows), ["order_id"])
    merge(spark, settings.path("gold/fact_order_item"), item_fact(item_rows, orders), ["order_item_id"])
    merge(spark, settings.path("gold/dim_date"), date_dimension(order_rows), ["date_key"])
    for table, transform, key, destination in (
        ("customers", customer_dimension, "customer_id", "dim_customer"),
        ("products", product_dimension, "product_id", "dim_product"),
    ):
        current = read_delta(spark, settings.path(f"silver/{table}"))
        selected = current.join(changes[table].select(key).distinct(), key, "left_semi")
        merge(spark, settings.path(f"gold/{destination}"), transform(selected), [key.replace("_id", "_key")])
