"""Real PostgreSQL -> Debezium -> Kafka -> original Spark pipeline load steps."""

import datetime as dt
import importlib.util
import json
import subprocess
import time
from pathlib import Path
from controller import BASE, ROOT, call, inspect, sample, save_log, log, wait


def compose(*args, timeout=90):
    r = call(BASE + list(args), timeout=timeout)
    if r.returncode:
        raise RuntimeError("Compose " + str(args) + ": " + r.stderr[-2000:])
    return r.stdout


def sql(query):
    return compose(
        "exec",
        "-T",
        "transactions-db",
        "psql",
        "-X",
        "-qAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "finance_db_user",
        "-d",
        "finance_db",
        "-c",
        query,
    ).strip()


LAST_RECOVERY = {}


def supervise():
    # Pipeline has its original unless-stopped policy; other services are managed here.
    for service in ("transactions-db", "kafka", "kafka-connect", "pipeline"):
        name = "deel-stress-" + service + "-1"
        state = inspect(name)
        if state is None:
            continue
        if state["status"] in ("exited", "dead"):
            now = time.monotonic()
            if now - LAST_RECOVERY.get(name, 0) < 30:
                continue
            LAST_RECOVERY[name] = now
            save_log(name)
            log("recovery.jsonl", dict(action="restart", **state))
            print("Supervisor restarting " + name, flush=True)
            r = call(["docker", "start", name], timeout=60)
            if r.returncode:
                log("recovery.jsonl", dict(name=name, error=r.stderr))
    # A FAILED Connect task does not necessarily crash its container.
    r = call(
        BASE
        + [
            "exec",
            "-T",
            "kafka-connect",
            "curl",
            "-fsS",
            "http://localhost:8083/connectors/finance-db-connector/status",
        ],
        timeout=10,
    )
    if r.returncode == 0:
        state = json.loads(r.stdout)
        if any(t.get("state") == "FAILED" for t in state.get("tasks", [])):
            name = "connect-task"
            if time.monotonic() - LAST_RECOVERY.get(name, 0) > 30:
                LAST_RECOVERY[name] = time.monotonic()
                log(
                    "recovery.jsonl",
                    dict(action="restart_failed_connect_task", state=state),
                )
                call(
                    BASE
                    + [
                        "exec",
                        "-T",
                        "kafka-connect",
                        "curl",
                        "-fsS",
                        "-X",
                        "POST",
                        "http://localhost:8083/connectors/finance-db-connector/restart?includeTasks=true&onlyFailed=true",
                    ],
                    timeout=10,
                )


def telemetry():
    code = """import json
from pathlib import Path
p=Path('/lake/online/telemetry/batches.jsonl')
records=[]
if p.exists():
 for line in p.read_text().splitlines():
  try: records.append(json.loads(line))
  except ValueError: pass
latest={r['batch_id']:r for r in records if r.get('status')=='committed' and 'input_events' in r}
progress=[]
for p in Path('/lake/online/observability/progress').glob('kafka_to_bronze*.json'):
 try: progress.append(json.loads(p.read_text()))
 except ValueError: pass
bybatch={}
for r in sorted(progress,key=lambda x:x.get('timestamp','')): bybatch[r['batchId']]=r
print(json.dumps(dict(materialized=sum(r['input_events'] for r in latest.values()),
 bronze=sum(r.get('numInputRows',0) for r in bybatch.values()),
 last_batch=max(latest,default=-1),last_record=latest.get(max(latest,default=-1)),
 bronze_progress=max(progress,key=lambda x:x['batchId'],default=None))))
"""
    r = call(BASE + ["exec", "-T", "pipeline", "python3", "-c", code], timeout=12)
    if r.returncode:
        return {}
    try:
        return json.loads(r.stdout)
    except ValueError:
        return {}


def kafka_count():
    r = call(
        BASE
        + [
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            "finance_db.operations.*",
            "--time",
            "-1",
        ],
        timeout=15,
    )
    if r.returncode:
        return None
    try:
        return sum(
            int(line.rsplit(":", 1)[1])
            for line in r.stdout.splitlines()
            if line.startswith("finance_db.operations.")
        )
    except ValueError:
        return None


def checkpoint(phase, produced, expected):
    supervise()
    names = [
        "deel-stress-" + s + "-1"
        for s in ("transactions-db", "kafka", "kafka-connect", "pipeline")
    ]
    sample(names)
    t = telemetry()
    k = kafka_count()
    row = dict(phase=phase, produced=produced, expected=expected, kafka_events=k, **t)
    if k is not None and t:
        row["kafka_to_bronze_backlog"] = max(0, k - t.get("bronze", 0))
        row["bronze_to_gold_backlog"] = max(
            0, t.get("bronze", 0) - t.get("materialized", 0)
        )
        row["source_to_kafka_backlog"] = max(0, expected - k)
    log("online-samples.jsonl", row)
    print(
        "Sample "
        + phase
        + " "
        + json.dumps(
            {
                key: row.get(key)
                for key in (
                    "produced",
                    "kafka_events",
                    "bronze",
                    "materialized",
                    "kafka_to_bronze_backlog",
                    "bronze_to_gold_backlog",
                )
            }
        ),
        flush=True,
    )
    return row


def drain(phase, expected, timeout=420):
    start = time.monotonic()
    last = None
    while time.monotonic() - start < timeout:
        last = checkpoint(phase, 0, expected)
        if (
            last.get("materialized", 0) >= expected
            and last.get("kafka_events", 0) >= expected
        ):
            log(
                "drains.jsonl",
                dict(
                    phase=phase,
                    expected=expected,
                    drain_seconds=time.monotonic() - start,
                    status="caught_up",
                ),
            )
            return True
        time.sleep(8)
    log(
        "drains.jsonl",
        dict(
            phase=phase,
            expected=expected,
            drain_seconds=time.monotonic() - start,
            status="timeout",
            last=last,
        ),
    )
    return False


NEXT_ORDER = 1000000000


def produce_orders(n):
    global NEXT_ORDER
    a = NEXT_ORDER
    b = a + n - 1
    NEXT_ORDER = b + 1
    # IDs are confined to the isolated source. Five item events plus one order event.
    query = f"""BEGIN;
INSERT INTO operations.orders(order_id,order_date,delivery_date,customer_id,status)
SELECT id,CURRENT_DATE,CURRENT_DATE+CAST(id%30 AS INT),1,'PENDING' FROM generate_series({a}::bigint,{b}::bigint) id WHERE true ON CONFLICT DO NOTHING;
INSERT INTO operations.order_items(order_item_id,order_id,product_id,quanity)
SELECT id*10+j,id,1,5 FROM generate_series({a}::bigint,{b}::bigint) id CROSS JOIN generate_series(1,5) j WHERE true ON CONFLICT DO NOTHING;
COMMIT;"""
    for attempt in range(8):
        try:
            sql(query)
            return n * 6
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            log(
                "recovery.jsonl",
                dict(
                    action="retry_source_transaction",
                    attempt=attempt,
                    first_order=a,
                    last_order=b,
                    error=str(exc)[:500],
                ),
            )
            supervise()
            time.sleep(3)
    raise RuntimeError("Source transaction did not recover")


def run_query(name):
    cname = "deel-stress-" + name
    started = time.monotonic()
    compose("run", "-d", "--no-deps", "--name", cname, "query", "query", "--name", name)
    state = wait(cname, timeout=240)
    state["invocation_wall_seconds"] = time.monotonic() - started
    log("query-runs.jsonl", state)
    if state["exit_code"] != 0:
        raise RuntimeError("Query failed " + name)
    return state


RECON_IDX = 0


def reconcile():
    global RECON_IDX
    RECON_IDX += 1
    name = "reconciliation-" + str(RECON_IDX)
    spec = importlib.util.spec_from_file_location(
        "reference", Path("scripts/verify_live.py")
    )
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)
    reference.compose = lambda *args, input=None: compose(*args)
    expected = reference.source_metrics()
    run_query(name)
    text = (ROOT / "logs" / ("deel-stress-" + name + ".log")).read_text()
    records = [
        json.loads(line[7:]) for line in text.splitlines() if line.startswith("STRESS ")
    ]
    actual = {k: v["rows"] for k, v in records[-1]["metrics"].items()}
    ok = actual == expected
    log(
        "reconciliation.jsonl",
        dict(
            passed=ok,
            expected=expected,
            actual=actual,
            ordercounts=records[-1]["ordercounts"],
            itemcounts=records[-1]["itemcounts"],
        ),
    )
    if not ok:
        raise AssertionError("Stress source/Gold reconciliation mismatch")
    print("All four business metrics reconciled successfully.", flush=True)


def main():
    # Only stress services; original project remains stopped.
    compose(
        "up",
        "-d",
        "--no-build",
        "--wait",
        "--wait-timeout",
        "120",
        "transactions-db",
        "kafka",
        timeout=160,
    )
    sql("UPDATE cron.job SET active=false;")
    baseline = int(
        sql(
            "SELECT (SELECT COUNT(*) FROM operations.customers)+(SELECT COUNT(*) FROM operations.products)+(SELECT COUNT(*) FROM operations.orders)+(SELECT COUNT(*) FROM operations.order_items)"
        )
    )
    compose(
        "up",
        "-d",
        "--no-build",
        "kafka-connect",
        "debezium-init",
        "pipeline",
        timeout=160,
    )
    expected = baseline
    total_produced = 0
    if not drain("initial-snapshot", expected, timeout=180):
        raise RuntimeError("Snapshot did not catch up")
    # The cap is 20k Kafka offsets / 10 s, so levels straddle the nominal 2k/s cap.
    for rate in (300, 1200, 3000, 6000):
        phase = "rate-" + str(rate)
        start = time.monotonic()
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        next_sample = start
        emitted = 0
        sql_seconds = 0
        ticks = 0
        while time.monotonic() - start < 120:
            tick = time.monotonic()
            work = produce_orders(rate // 6)
            sql_seconds += time.monotonic() - tick
            emitted += work
            total_produced += work
            expected += work
            ticks += 1
            if time.monotonic() >= next_sample:
                checkpoint(phase, emitted, expected)
                next_sample = time.monotonic() + 15
            time.sleep(max(0, 1 - (time.monotonic() - tick)))
        elapsed = time.monotonic() - start
        load_ended_at = dt.datetime.now(dt.timezone.utc).isoformat()
        end = checkpoint(phase, emitted, expected)
        log(
            "rate-levels.jsonl",
            dict(
                phase=phase,
                target_events_per_second=rate,
                emitted_events=emitted,
                elapsed_seconds=elapsed,
                started_at_utc=started_at,
                load_ended_at_utc=load_ended_at,
                actual_events_per_second=emitted / elapsed,
                source_sql_seconds=sql_seconds,
                transactions=ticks,
                ending_sample=end,
            ),
        )
        if not drain(phase + "-drain", expected, timeout=420):
            print(
                "Recovery window exceeded; stopping further source load and retaining backlog.",
                flush=True,
            )
            if not drain("extended-recovery", expected, timeout=600):
                raise RuntimeError("Pipeline did not recover backlog")
            break
    reconcile()
    # Exercise mutations on the accumulated history, then reconcile again.
    a = 1000000000
    b = NEXT_ORDER - 1
    result = sql(
        f"WITH changed AS (UPDATE operations.orders SET status='COMPLETED',updated_at=CURRENT_TIMESTAMP WHERE order_id BETWEEN {a} AND {b} AND order_id%10=0 RETURNING 1) SELECT COUNT(*) FROM changed"
    )
    changed = int(result)
    expected += changed
    result = sql(
        f"WITH changed AS (DELETE FROM operations.order_items WHERE order_id BETWEEN {a} AND {b} AND order_item_id%97=0 RETURNING 1) SELECT COUNT(*) FROM changed"
    )
    deleted = int(result)
    expected += 2 * deleted
    log(
        "mutations.jsonl",
        dict(
            order_updates=changed,
            item_deletes=deleted,
            expected_tombstones=deleted,
            expected=expected,
        ),
    )
    if not drain("updates-deletes", expected, timeout=420):
        raise RuntimeError("Mutation backlog did not drain")
    reconcile()
    # Controlled outage in the isolated broker; source keeps accepting changes.
    failure_start = time.monotonic()
    before = inspect("deel-stress-kafka-1")
    compose("stop", "--timeout", "30", "kafka", timeout=45)
    expected += produce_orders(1000)
    time.sleep(10)
    # The supervisor, not a blanket stack reset, restores the stopped service.
    supervise()
    recovered = drain("broker-recovery", expected, timeout=300)
    log(
        "broker-outage.jsonl",
        dict(
            downtime_requested_seconds=10,
            source_events_during_outage=6000,
            recovery_seconds=time.monotonic() - failure_start,
            caught_up=recovered,
            before=before,
            after=inspect("deel-stress-kafka-1"),
            pipeline=inspect("deel-stress-pipeline-1"),
        ),
    )
    if not recovered:
        raise RuntimeError("Broker outage did not recover")
    reconcile()
    # Query concurrency while the pipeline is live but the synthetic source is quiet.
    for concurrency in (1, 2, 4):
        names = []
        started = time.monotonic()
        for i in range(concurrency):
            name = f"cli-c{concurrency}-{i}"
            names.append("deel-stress-" + name)
            compose(
                "run",
                "-d",
                "--no-deps",
                "--name",
                names[-1],
                "query",
                "query",
                "--name",
                name,
            )
        while any((inspect(n) or {}).get("status") == "running" for n in names):
            sample(names + ["deel-stress-pipeline-1"])
            supervise()
            time.sleep(4)
            if time.monotonic() - started > 240:
                raise RuntimeError("CLI concurrency timed out")
        states = []
        for name in names:
            save_log(name)
            states.append(inspect(name))
        log(
            "query-concurrency.jsonl",
            dict(
                concurrency=concurrency,
                group_seconds=time.monotonic() - started,
                containers=states,
            ),
        )
        if any(s["exit_code"] != 0 for s in states):
            for s in states:
                if s["exit_code"] != 0:
                    call(["docker", "start", s["name"]], timeout=30)
                    wait(s["name"], timeout=180)
    # Persist raw telemetry for analysis and stop only the isolated stack at end.
    compose(
        "cp", "pipeline:/lake/online", str(ROOT / "results/online-lake"), timeout=120
    )
    for service in ("transactions-db", "kafka", "kafka-connect", "pipeline"):
        save_log("deel-stress-" + service + "-1")
    log(
        "completion.jsonl",
        dict(status="finished", total_source_events=total_produced, expected=expected),
    )
    print("Online stress campaign finished.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException as exc:
        log("completion.jsonl", dict(status="failed", error=str(exc)))
        for service in ("transactions-db", "kafka", "kafka-connect", "pipeline"):
            if inspect("deel-stress-" + service + "-1"):
                save_log("deel-stress-" + service + "-1")
        raise
