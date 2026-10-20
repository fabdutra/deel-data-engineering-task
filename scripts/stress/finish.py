"""Final independent checks, evidence copy, and stop only the stress project."""

import json
import subprocess
import sys
from pathlib import Path
from controller import BASE, ROOT, call, inspect, save_log, log, wait
from online_controller import compose, sql, kafka_count, telemetry

completion = ROOT / "results/completion.jsonl"
assert (
    completion.exists()
    and json.loads(completion.read_text().splitlines()[-1])["status"] == "finished"
)
source = json.loads(
    sql(
        """SELECT json_build_object(
 'ordercounts',(SELECT json_build_object('total',COUNT(*),'completed',COUNT(*) FILTER(WHERE status='COMPLETED')) FROM operations.orders),
 'itemcounts',(SELECT json_build_object('total',COUNT(*),'units',SUM(quanity)) FROM operations.order_items))"""
    )
)
recon = json.loads((ROOT / "results/reconciliation.jsonl").read_text().splitlines()[-1])
actual = {k: recon[k] for k in ("ordercounts", "itemcounts")}
log(
    "aggregate-integrity.jsonl",
    dict(source=source, gold=actual, passed=source == actual),
)
assert source == actual, (source, actual)
t = telemetry()
k = kafka_count()
log("final-offsets.jsonl", dict(kafka_events=k, **t))
assert k == t["bronze"] == t["materialized"], (k, t)
# Freeze writer gracefully before the physical-file verification.
compose("stop", "--timeout", "45", "pipeline", timeout=60)
name = "deel-stress-online-integrity"
compose(
    "run",
    "-d",
    "--no-deps",
    "--name",
    name,
    "test",
    "online-check",
    "--expected-count",
    str(k),
)
state = wait(name, timeout=180)
assert state["exit_code"] == 0, state
save_log("deel-stress-bronze-check")
# Refresh copied telemetry after the physical verification.
compose(
    "cp",
    "pipeline:/lake/online/telemetry",
    str(ROOT / "results/final-telemetry"),
    timeout=120,
)
code = """import json
from pathlib import Path
out=[]
for p in Path('/events').iterdir():
 if not p.is_file(): continue
 try:
  with p.open() as f:
   for i in range(12):
    line=f.readline()
    if not line: break
    e=json.loads(line)
    if e.get('Event')=='SparkListenerApplicationStart':
     if e.get('App Name','').startswith(('stress-online','stress-query','stress-offline')): out.append(dict(path=str(p),name=e['App Name']))
     break
 except (ValueError,OSError): pass
print(json.dumps(out))
"""
# Pipeline is stopped; use the exited integrity container for docker cp and a tiny helper for listing.
r = call(
    BASE + ["run", "--rm", "--no-deps", "--entrypoint", "python3", "test", "-c", code],
    timeout=60,
)
assert r.returncode == 0, r.stderr[-2000:]
events = json.loads(r.stdout)
dest = ROOT / "results/live-event-logs"
dest.mkdir(exist_ok=True)
for event in events:
    r = call(
        [
            "docker",
            "cp",
            name + ":" + event["path"],
            str(dest / Path(event["path"]).name),
        ],
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
log("live-applications.jsonl", dict(applications=events))
services = ("transactions-db", "kafka", "kafka-connect", "pipeline")
before = [inspect("deel-stress-" + s + "-1") for s in services]
for s in services:
    save_log("deel-stress-" + s + "-1")
compose("stop", "--timeout", "45", timeout=90)
after = [inspect("deel-stress-" + s + "-1") for s in services]
log("final-state.jsonl", dict(before=before, after=after, integrity="passed"))
running = {}
for project in ("deel-assessment", "deel-stress"):
    r = call(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project=" + project,
            "--format",
            "{{.Names}}",
        ],
        timeout=15,
    )
    assert r.returncode == 0, r.stderr
    running[project] = r.stdout.splitlines()
log("running-projects-final.jsonl", dict(running=running))
assert not any(running.values()), running
print(
    "Final integrity checks passed; isolated stack stopped. Volumes preserved.",
    flush=True,
)
