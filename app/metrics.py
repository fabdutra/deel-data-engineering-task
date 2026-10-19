import datetime as dt
import json
from pathlib import Path
from pyspark.sql import functions as F
from app.storage import delta_exists, read_delta

METRICS = ("open-orders", "top-delivery-dates", "pending-items", "top-customers")


def load_publication(spark, settings):
    path = settings.path("gold/publication")
    if not delta_exists(spark, path):
        raise RuntimeError("No Gold publication yet. Check pipeline logs and retry after the first batch.")
    return read_delta(spark, path).filter("id = 1").first()


def register_views(spark, settings, publication):
    for name, version in publication["versions"].items():
        data = read_delta(spark, settings.path(f"gold/{name}"), version)
        if "_deleted" in data.columns:
            data = data.filter(~F.col("_deleted"))
        data.createOrReplaceTempView(name)


def result(spark, metric):
    if metric not in METRICS:
        raise ValueError(f"Unknown metric {metric}")
    query = (Path(__file__).resolve().parents[1] / "sql" / f"{metric}.sql").read_text()
    return spark.sql(query)


def serve(spark, settings, metric="all", output="table", limit=100):
    publication = load_publication(spark, settings)
    register_views(spark, settings, publication)
    metadata = publication.asDict(recursive=True)
    metadata["queried_at"] = dt.datetime.now(dt.timezone.utc)
    if output == "jsonl":
        print(json.dumps({"publication": metadata}, default=str), flush=True)
    else:
        print(f"Publication: batch={publication['batch_id']} published_at={publication['published_at']} UTC")
        print(f"Table display limit: {limit}; use --format jsonl to export every result row.")
    for name in METRICS if metric == "all" else (metric,):
        data = result(spark, name)
        if output == "jsonl":
            for row in data.toLocalIterator():
                print(json.dumps({"metric": name, "row": row.asDict()}, default=str), flush=True)
        else:
            print(f"\n{name}")
            data.show(limit, truncate=False)
