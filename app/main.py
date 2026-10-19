import argparse
import logging
import sys
from app.config import Settings
from app.metrics import METRICS
from app.session import session


def main():
    parser = argparse.ArgumentParser(description="ACME streaming analytics")
    commands = parser.add_subparsers(dest="command", required=True)
    pipeline = commands.add_parser("pipeline")
    pipeline.add_argument("--available-now", action="store_true")
    metrics = commands.add_parser("metrics")
    metrics.add_argument("metric", choices=("all", *METRICS), default="all", nargs="?")
    metrics.add_argument("--format", choices=("table", "jsonl"), default="table")
    metrics.add_argument("--limit", type=int, default=100)
    commands.add_parser("test")
    benchmark = commands.add_parser("benchmark")
    benchmark.add_argument("--orders", type=int, default=10000)
    benchmark.add_argument("--items-per-order", type=int, default=5)
    args = parser.parse_args()
    if args.command == "metrics" and args.limit < 1:
        parser.error("--limit must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True, stream=sys.stderr)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = Settings()
    spark = session(settings, f"acme-{args.command}")
    try:
        if args.command == "pipeline":
            from app.pipeline import run
            run(spark, settings, args.available_now)
        elif args.command == "metrics":
            from app.metrics import serve
            serve(spark, settings, args.metric, args.format, args.limit)
        elif args.command == "benchmark":
            from app.benchmark import run
            run(spark, settings, args.orders, args.items_per_order)
        else:
            import unittest
            suite = unittest.defaultTestLoader.discover("/opt/deel/tests", pattern="test_*.py")
            if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
                return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
