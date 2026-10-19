"""Reconcile all business metrics against a temporarily quiesced source.

Only run against this assessment's synthetic database. Restores cron settings and
any modified order in finally. Does not print environment variables or credentials.
"""
import datetime as dt
import json
import subprocess
import time
from pathlib import Path


def compose(*args, input=None):
    completed = subprocess.run(["docker", "compose", *args], input=input, text=True,
                               capture_output=True, check=True)
    return completed.stdout


def sql(query):
    output = compose("exec", "-T", "transactions-db", "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                     "-U", "finance_db_user", "-d", "finance_db", "-c", query)
    return output.strip()


def source_metrics():
    opened = "status IN ('PENDING','PROCESSING','REPROCESSING')"
    queries = {
        "open-orders": f"SELECT delivery_date, status, COUNT(*) AS open_orders FROM operations.orders WHERE {opened} GROUP BY delivery_date, status ORDER BY delivery_date NULLS LAST, status",
        "top-delivery-dates": f"SELECT delivery_date, COUNT(*) AS open_orders FROM operations.orders WHERE {opened} GROUP BY delivery_date ORDER BY open_orders DESC, delivery_date ASC NULLS LAST LIMIT 3",
        "pending-items": f"SELECT i.product_id, COUNT(*) AS pending_item_lines, SUM(i.quanity) AS pending_units FROM operations.order_items i JOIN operations.orders o USING(order_id) WHERE o.{opened} GROUP BY i.product_id ORDER BY i.product_id",
        "top-customers": f"WITH counts AS (SELECT customer_id, COUNT(*) AS pending_orders FROM operations.orders WHERE {opened} GROUP BY customer_id) SELECT c.customer_id, d.customer_name, c.pending_orders FROM counts c LEFT JOIN operations.customers d USING(customer_id) ORDER BY pending_orders DESC, customer_id ASC LIMIT 3",
    }
    return {name: json.loads(sql(f"SELECT COALESCE(json_agg(t), '[]'::json) FROM ({query}) t")) for name, query in queries.items()}


def gold_metrics():
    output = compose("run", "--rm", "--no-deps", "query", "metrics", "all", "--format", "jsonl")
    rows = {name: [] for name in ("open-orders", "top-delivery-dates", "pending-items", "top-customers")}
    publication = None
    for line in output.splitlines():
        if line.startswith('{'):
            record = json.loads(line)
            if "publication" in record:
                publication = record["publication"]
            else:
                rows[record["metric"]].append(record["row"])
    if publication is None:
        raise RuntimeError("CLI produced no publication")
    return rows, publication


def await_match(expected, timeout=180):
    started = time.monotonic()
    last_error = None
    while time.monotonic() - started < timeout:
        try:
            actual, publication = gold_metrics()
            if actual == expected:
                return actual, publication
            last_error = f"Expected {expected}, received {actual}"
        except subprocess.CalledProcessError as exc:
            last_error = exc.stderr[-1500:]
        time.sleep(2)
    raise AssertionError(f"Source/Gold did not converge: {last_error}")


def main():
    active_jobs = json.loads(sql("SELECT COALESCE(json_agg(jobid), '[]'::json) FROM cron.job WHERE active"))
    changed = None
    report = {"timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        sql("UPDATE cron.job SET active=false WHERE active")
        # Existing short synthetic generator calls can finish before reconciliation.
        time.sleep(3)
        expected = source_metrics()
        initial, publication = await_match(expected)
        report["initial_reconciliation"] = "passed_all_four_metrics"
        report["initial_publication"] = publication
        compose("restart", "pipeline")
        selected = sql("SELECT order_id || ',' || status FROM operations.orders WHERE status IN ('PENDING','PROCESSING','REPROCESSING') ORDER BY order_id LIMIT 1")
        if not selected:
            raise RuntimeError("No open order available for restart/update test")
        order_id, old_status = selected.split(',')
        changed = (int(order_id), old_status)
        started = time.monotonic()
        sql(f"UPDATE operations.orders SET status='COMPLETED', updated_at=CURRENT_TIMESTAMP WHERE order_id={int(order_id)}")
        expected = source_metrics()
        actual, publication = await_match(expected)
        report["restart_then_cdc_update"] = "passed_all_four_metrics"
        report["update_visibility_seconds_upper_bound"] = round(time.monotonic() - started, 3)
        report["latency_note"] = "Includes pipeline restart, CLI cold starts and polling; not a steady-state percentile."
        report["changed_order_id"] = int(order_id)
        report["final_publication"] = publication
        report["metrics"] = actual
        target = Path("docs/evidence/live-reconciliation.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2), flush=True)
    finally:
        if changed:
            order_id, status = changed
            if status not in ("PENDING", "PROCESSING", "REPROCESSING"):
                raise ValueError("Unexpected source status")
            sql(f"UPDATE operations.orders SET status='{status}', updated_at=CURRENT_TIMESTAMP WHERE order_id={order_id}")
        if active_jobs:
            ids = ','.join(str(int(i)) for i in active_jobs)
            sql(f"UPDATE cron.job SET active=true WHERE jobid IN ({ids})")


if __name__ == "__main__":
    main()
