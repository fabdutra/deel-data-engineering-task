from pyspark.sql import SparkSession


def session(settings, name="acme-analytics"):
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.databricks.delta.snapshotPartitions", "2")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .config("spark.sql.streaming.stopTimeout", "30000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
