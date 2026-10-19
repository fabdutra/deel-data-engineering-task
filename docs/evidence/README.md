# Recorded execution evidence

These are actual local executions on ARM64 with Spark 3.5.8, Delta 3.3.2 and the Compose configuration in this repository. Machine-readable evidence uses UTC timestamps; the local interview preparation timezone is America/Sao_Paulo. Data is synthetic. Measurements are observations, not production capacity guarantees.

| Artifact | What it proves |
|---|---|
| `integration-tests.log` | Real Spark/Delta scenario passed, including separate foreachBatch sessions, crash/replay, ordering and deletion guards |
| `live-reconciliation.json` | All four CLI metrics matched PostgreSQL before and after a pipeline restart followed by a CDC update |
| `cli-metrics.txt` | Human-readable metrics from the live stack, with publication timestamp |
| `benchmark-100k-orders.json` | 100,000 orders, 500,000 items and 200 dimension events materialized; 10,000 order updates applied |
| `benchmark-spark-tasks.json` | CPU, task-duration, GC, shuffle and spill metrics extracted from that benchmark's Spark event log |
| `streaming-progress-summary.jsonl` | Native nonempty streaming-batch durations; reported input rows can include repeated scans |

The synthetic benchmark wrote Bronze in **1.567 s**, materialized **600,200 events in 43.764 s** (about **13,714 unique input events/s** for that job), and applied **10,000 order updates in 13.111 s**. Those updates also refresh their 50,000 associated item facts. This was an isolated materialization workload, not Kafka ingestion throughput, and its target tables are smaller than a production lake.

The event log records **585 task attempts**, **0 failed task attempts**, **0 memory/disk spill**, approximately **46.9 executor CPU seconds**, and **66.1 MB shuffle written**. Summed executor run time can exceed wall time because tasks run concurrently. Input bytes include repeated scans/cached reads; they are not unique storage bytes. JVM GC time is reported per task and can overlap across concurrent tasks.

The live restart-followed-by-update test observed an upper bound of **46.561 s** from the source update through a matching CLI response. That includes pipeline restart, CLI cold starts and polling. It is not steady-state p95 latency. In the retained native progress sample, materialization's reported p95 batch duration was **19.347 s** with a **29.825 s** maximum. The sample is small and includes catch-up/startup activity; it does not establish an SLA.

`pending-items` happened to be empty in the live snapshot and was independently confirmed empty at the source. The starter generator can attach item rows to older completed orders. The deterministic integration fixture separately verifies nonempty pending line/unit counts and status propagation; the empty live output is not used as sole proof of this metric.

A separate disposable PostgreSQL project was booted with a fresh volume: the CDC replication user, publication, source tables and seed orders were created successfully. Only that test project's resources were removed afterward.

Reproduce with the validation commands in the root README. Raw progress and event logs remain in Docker volumes; large raw logs and benchmark fixtures are intentionally excluded from Git. The JSON/CLI artifacts can correspond to different publications because the source changes continuously.
