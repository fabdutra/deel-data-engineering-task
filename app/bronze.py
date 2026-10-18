from pyspark.sql import functions as F
from app.schemas import BRONZE_SCHEMA, TOPICS


def start_bronze(spark, settings, available_now=False):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.bootstrap_servers)
           .option("subscribe", TOPICS)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")
           .option("maxOffsetsPerTrigger", settings.max_offsets).load())
    envelope = raw.select("key", "value", "topic", "partition", "offset",
                          F.col("timestamp").alias("kafka_timestamp"),
                          F.current_timestamp().alias("ingested_at"))
    writer = (envelope.writeStream.format("parquet").outputMode("append")
              .queryName("kafka_to_bronze")
              .option("path", settings.path("bronze/events"))
              .option("checkpointLocation", settings.path("checkpoints/bronze"))
              .option("compression", "snappy")
              .option("maxRecordsPerFile", 1000000))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return writer.start()


def bronze_reader(spark, settings):
    return (spark.readStream.schema(BRONZE_SCHEMA).format("parquet")
            .option("maxFilesPerTrigger", settings.max_files)
            .load(settings.path("bronze/events")))
