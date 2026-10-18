import logging
import signal
import time
from pathlib import Path
from app.bronze import bronze_reader, start_bronze
from app.materialize import initialize, materialize_batch
from app.observability import ProgressRecorder

LOG = logging.getLogger(__name__)


def run(spark, settings, available_now=False):
    initialize(spark, settings)
    listener = ProgressRecorder(settings.lake_root)
    spark.streams.addListener(listener)
    queries = []
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        queries.append(start_bronze(spark, settings, available_now))
        # File source needs its root to exist. Never create a fake Parquet file.
        path = Path(settings.path("bronze/events"))
        path.mkdir(parents=True, exist_ok=True)
        writer = (bronze_reader(spark, settings).writeStream.queryName("bronze_to_gold")
                  .option("checkpointLocation", settings.path("checkpoints/materialize"))
                  .foreachBatch(lambda batch, batch_id: materialize_batch(spark, settings, batch, batch_id)))
        if available_now:
            queries[0].awaitTermination()
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
        queries.append(writer.start())
        while not stopping:
            for query in queries:
                error = query.exception()
                if error:
                    raise error
            if available_now and all(not q.isActive for q in queries):
                break
            if not available_now and any(not q.isActive for q in queries):
                raise RuntimeError("A streaming query stopped unexpectedly")
            time.sleep(1)
    finally:
        for query in reversed(queries):
            if query.isActive:
                query.stop()
        spark.streams.removeListener(listener)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
