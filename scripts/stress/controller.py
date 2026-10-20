"""Host-side supervisor. Only manages containers in the deel-stress project."""

import datetime as dt
import json
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
for directory in ("results", "logs"):
    (ROOT / directory).mkdir(exist_ok=True)
BASE = [
    "docker",
    "compose",
    "-p",
    "deel-stress",
    "-f",
    "docker-compose.yaml",
    "-f",
    "scripts/stress/compose.yaml",
]


def call(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, **kwargs)


def log(name, record):
    record["host_time_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    with (ROOT / "results" / name).open("a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def inspect(name):
    r = call(["docker", "inspect", name])
    if r.returncode:
        return None
    d = json.loads(r.stdout)[0]
    return dict(
        name=name,
        status=d["State"]["Status"],
        exit_code=d["State"]["ExitCode"],
        oom=d["State"].get("OOMKilled", False),
        error=d["State"].get("Error"),
        restarts=d.get("RestartCount", 0),
        started=d["State"]["StartedAt"],
        finished=d["State"]["FinishedAt"],
    )


def sample(names):
    if not names:
        return
    try:
        r = call(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        log("monitor-errors.jsonl", dict(action="stats_timeout", names=names))
        return
    if r.returncode:
        return
    for line in r.stdout.splitlines():
        try:
            log("resources.jsonl", json.loads(line))
        except json.JSONDecodeError:
            pass


def save_log(name):
    r = call(["docker", "logs", name])
    (ROOT / "logs" / f"{name}.log").write_text(r.stdout + "\n" + r.stderr)


def wait(name, timeout=1200):
    start = time.monotonic()
    next_sample = 0
    while time.monotonic() - start < timeout:
        state = inspect(name)
        if state is None:
            raise RuntimeError("Missing test container " + name)
        if state["status"] in ("exited", "dead"):
            save_log(name)
            state["observed_seconds"] = time.monotonic() - start
            log("containers.jsonl", state)
            print(json.dumps(state), flush=True)
            return state
        if time.monotonic() >= next_sample:
            sample([name])
            next_sample = time.monotonic() + 5
        time.sleep(1)
    # Cancel only this test container, retaining its volume/logs for diagnosis.
    call(["docker", "stop", "--time", "30", name], timeout=40)
    save_log(name)
    state = inspect(name)
    state["supervisor_timeout"] = True
    log("containers.jsonl", state)
    return state


def run_offline():
    cases = [
        ("scale-100k", 100000, 5, False),
        ("scale-500k", 500000, 5, False),
        ("scale-1m", 1000000, 5, False),
        ("hot-1m-items", 1, 1000000, True),
    ]
    for name, n, m, hot in cases:
        cname = "deel-stress-" + name
        state = inspect(cname)
        if state is None:
            cmd = BASE + [
                "run",
                "-d",
                "--no-deps",
                "--name",
                cname,
                "test",
                "offline",
                "--name",
                name,
                "--orders",
                str(n),
                "--items",
                str(m),
            ]
            if hot:
                cmd += ["--hot"]
            r = call(cmd, timeout=120)
            if r.returncode:
                raise RuntimeError(r.stderr)
        print("Observing " + cname, flush=True)
        state = wait(cname)
        if state["exit_code"] != 0:
            # Keep the failed experiment and restart the identical case once.
            print(
                "Restarting failed case once with the same resources: " + cname,
                flush=True,
            )
            r = call(["docker", "start", cname], timeout=30)
            if r.returncode:
                raise RuntimeError(r.stderr)
            retry = wait(cname)
            retry["retry_of"] = name
            log("recovery.jsonl", retry)
            # A repeatable failure bounds this workload. Continue to the next case.
    r = call(BASE + ["cp", "test:/lake", str(ROOT / "results/offline-lake")])
    # One-off runners do not have a canonical service container; copy from last one.
    if r.returncode:
        r = call(
            [
                "docker",
                "cp",
                "deel-stress-hot-1m-items:/lake",
                str(ROOT / "results/offline-lake"),
            ]
        )
    print("Offline matrix finished; lake copy status=" + str(r.returncode), flush=True)


if __name__ == "__main__":
    run_offline()
