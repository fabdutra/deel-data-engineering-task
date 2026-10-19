from uuid import uuid4
from pyspark.sql import functions as F


def exists(spark, path):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).exists(
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )


def delta_exists(spark, path):
    return exists(spark, path + "/_delta_log")


def read_delta(spark, path, version=None):
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def merge(spark, path, source, keys, versioned=False):
    """Bounded source; target data remains distributed. Soft deletes keep ordering guards."""
    if source.isEmpty():
        return
    if not delta_exists(spark, path):
        source.write.format("delta").mode("errorifexists").save(path)
        return
    # foreachBatch DataFrames can belong to a different SparkSession than the
    # driver. Resolve the view in that same session, never in a stale driver view.
    view = "_merge_" + uuid4().hex
    session = source.sparkSession
    source.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    condition = " AND (s._lsn > t._lsn OR (s._lsn = t._lsn AND s._offset > t._offset))" if versioned else ""
    try:
        session.sql(f"""
            MERGE INTO delta.`{path}` t USING {view} s ON {on}
            WHEN MATCHED{condition} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        session.catalog.dropTempView(view)


def version(spark, path):
    return int(spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first()["version"])


def live(spark, path):
    return read_delta(spark, path).filter(~F.col("_deleted"))
