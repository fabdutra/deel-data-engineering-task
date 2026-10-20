#!/bin/bash
set -euo pipefail
export PYTHONPATH=/opt/deel:/stress
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/events \
  /stress/workload.py "$@"
