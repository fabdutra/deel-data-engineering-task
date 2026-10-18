from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dependency-check").getOrCreate()
spark.stop()
