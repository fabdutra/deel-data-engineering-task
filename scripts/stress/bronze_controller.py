import json
import time
from controller import BASE, ROOT, call, inspect, sample, save_log, log, wait
from online_controller import compose

BRONZE = "deel-stress-bronze-only"


def ingested():
    code = """import json
from pathlib import Path
rows=[]
for p in Path('/lake/bronze-only/observability/progress').glob('*.json'):
 try: rows.append(json.loads(p.read_text()))
 except ValueError: pass
latest={r['batchId']:r for r in sorted(rows,key=lambda r:r['timestamp'])}
print(json.dumps(dict(rows=sum(r['numInputRows'] for r in latest.values()),last=max(latest,default=-1))))
"""
    r = call(["docker", "exec", BRONZE, "python3", "-c", code], timeout=10)
    return json.loads(r.stdout) if r.returncode == 0 else dict(rows=0, last=-1)


def main():
    global BRONZE
    compose(
        "up", "-d", "--no-build", "--wait", "--wait-timeout", "90", "kafka", timeout=120
    )
    compose(
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka:29092",
        "--create",
        "--if-not-exists",
        "--topic",
        "stress.raw",
        "--partitions",
        "2",
        "--replication-factor",
        "1",
    )
    compose(
        "run", "-d", "--no-deps", "--name", "deel-stress-bronze-only", "test", "bronze"
    )
    expected = 0
    for size, records, cap in (
        (500, 250000, 20000),
        (5000, 100000, 20000),
        (500, 250000, 200000),
        (5000, 100000, 200000),
    ):
        if cap == 200000 and BRONZE == "deel-stress-bronze-only":
            save_log(BRONZE)
            call(["docker", "stop", "--time", "30", BRONZE], timeout=40)
            BRONZE = "deel-stress-bronze-relaxed"
            compose(
                "run",
                "-d",
                "--no-deps",
                "--name",
                BRONZE,
                "-e",
                "MAX_OFFSETS_PER_TRIGGER=200000",
                "test",
                "bronze",
            )
        name = "deel-stress-producer-" + str(size) + "-" + str(cap)
        start = time.monotonic()
        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            "deel-stress_default",
            "--cpus",
            "1",
            "--memory",
            "1g",
            "-e",
            "KAFKA_HEAP_OPTS=-Xms128m -Xmx256m",
            "--entrypoint",
            "/opt/kafka/bin/kafka-producer-perf-test.sh",
            "apache/kafka:3.9.1",
            "--topic",
            "stress.raw",
            "--num-records",
            str(records),
            "--record-size",
            str(size),
            "--throughput",
            "-1",
            "--producer-props",
            "bootstrap.servers=kafka:29092",
            "acks=all",
            "compression.type=none",
            "linger.ms=5",
        ]
        r = call(cmd, timeout=90)
        if r.returncode:
            raise RuntimeError(r.stderr)
        expected += records
        deadline = time.monotonic() + 360
        producer_state = None
        while time.monotonic() < deadline:
            state = inspect(name)
            for cname in ("deel-stress-kafka-1", BRONZE):
                check = inspect(cname)
                if check and check["status"] in ("exited", "dead"):
                    log("recovery.jsonl", dict(action="restart_during_bronze", **check))
                    save_log(cname)
                    call(["docker", "start", cname], timeout=30)
            got = ingested()
            sample(
                ["deel-stress-kafka-1", BRONZE]
                + ([name] if state["status"] == "running" else [])
            )
            log(
                "bronze-load-samples.jsonl",
                dict(
                    size=size,
                    offset_cap=cap,
                    expected=expected,
                    elapsed_seconds=time.monotonic() - start,
                    **got,
                ),
            )
            if state["status"] == "exited" and producer_state is None:
                producer_state = state
                save_log(name)
                log(
                    "producer-runs.jsonl",
                    dict(size=size, offset_cap=cap, records=records, **state),
                )
                if state["exit_code"] != 0:
                    call(["docker", "start", name], timeout=30)
                    # A restart may replay producer rows: this phase no longer has an exact count.
                    raise RuntimeError(
                        "Producer failed and was restarted; inspect logs before count reconciliation"
                    )
            if producer_state and got["rows"] >= expected:
                log(
                    "bronze-load-levels.jsonl",
                    dict(
                        size=size,
                        offset_cap=cap,
                        records=records,
                        total_expected=expected,
                        drain_complete_seconds=time.monotonic() - start,
                        ingested=got["rows"],
                        correctness=got["rows"] == expected,
                    ),
                )
                print(
                    "Bronze level completed " + str(size) + " bytes: " + str(got),
                    flush=True,
                )
                break
            time.sleep(7)
        else:
            raise RuntimeError("Bronze backlog did not drain")
    save_log(BRONZE)
    call(["docker", "stop", "--time", "30", BRONZE], timeout=40)
    compose(
        "run",
        "-d",
        "--no-deps",
        "--name",
        "deel-stress-bronze-check",
        "test",
        "bronze-check",
    )
    checked = wait("deel-stress-bronze-check", timeout=120)
    if checked["exit_code"] != 0:
        raise RuntimeError("Bronze verification failed")
    compose(
        "cp",
        "kafka:/var/lib/kafka/data/meta.properties",
        str(ROOT / "results/kafka-meta.properties"),
    )
    r = call(
        [
            "docker",
            "cp",
            BRONZE + ":/lake/bronze-only/observability",
            str(ROOT / "results/bronze-observability"),
        ],
        timeout=60,
    )
    print("Kafka/Bronze isolated test finished.", flush=True)


if __name__ == "__main__":
    main()
