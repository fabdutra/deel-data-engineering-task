from pyspark.sql import types as T

TABLES = {
    "customers": ("customer_id", "customer_id long, customer_name string, is_active boolean, customer_address string"),
    "products": ("product_id", "product_id long, product_name string, barcode string, unity_price string, is_active boolean"),
    "orders": ("order_id", "order_id long, order_date int, delivery_date int, customer_id long, status string"),
    "order_items": ("order_item_id", "order_item_id long, order_id long, product_id long, quanity int"),
}
TOPICS = ",".join(f"finance_db.operations.{name}" for name in TABLES)
BRONZE_SCHEMA = T.StructType([
    T.StructField("key", T.BinaryType()),
    T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
    T.StructField("ingested_at", T.TimestampType()),
])
OPEN_STATUSES = ("PENDING", "PROCESSING", "REPROCESSING")
ALL_STATUSES = (*OPEN_STATUSES, "COMPLETED")
GOLD_TABLES = ("dim_customer", "dim_product", "dim_date", "fact_order", "fact_order_item")
