# Spark sizing, performance and scale strategy

This document covers the assessment's two requested items: **Spark cluster sizing strategy** and **key Spark metrics used**. The [local sizing decision](#spark-cluster-sizing-strategy-local-deployment) is implemented and exercised; the [production sizing strategy](#spark-cluster-sizing-strategy-production) is a measurement-driven method, not a validated production cluster specification. The [metrics and their limits](#key-spark-metrics-used) are recorded below.

The subsequent [stress campaign](performance-stress.md) extends the initial evidence to six million input events, stepped CDC load, broker recovery and query concurrency. It found memory pressure/spill on large materializations and growing backlog above approximately 2,000 actual input events/s with the current intake cap. Completion and correctness were verified; a strict 30-second freshness SLA was not established.

## Spark cluster sizing strategy: local deployment

The implementation runs on an ARM64 development machine whose Docker VM has approximately 8 GB RAM and 10 available logical CPUs. The local Spark application uses `local[2]`: the driver and tasks share one JVM with two task slots, a 2 GiB heap and a 3 GiB container limit. This is explicitly a development sizing choice, not a production executor layout. Four shuffle partitions keep small joins/windows/merges parallel without the default 200-task overhead. Both streams share those slots.

| Service | Container memory cap | CPU cap | Reason |
|---|---:|---:|---|
| Spark pipeline | 3 GiB | 2 | 2 GiB heap plus Python, native and JVM overhead |
| PostgreSQL | 512 MiB | 1 | Small provided synthetic source |
| Kafka | 1 GiB | 1 | 512 MiB heap plus buffers/page cache |
| Kafka Connect | 1 GiB | 1 | 512 MiB heap plus connector overhead |
| One-off CLI | 1.5 GiB | 1 | 768 MiB heap; only allocated during a query |
| Test/benchmark | 3 GiB | 2 | Run separately for representative measurements |

The resource budgets were chosen to fit the local VM while leaving room for the source and CDC. Memory caps are limits, not reservations. The subsequent million-order stress fixture reached approximately the 3 GiB cgroup cap and recorded spill without OOM; ample memory headroom is not demonstrated. Avoid combining heavy benchmarks, History Server and extra query containers on an 8 GB VM. Startup batches/backfills are measured separately from warm batches. The near-real-time objective is a healthy pipeline that keeps up with arrivals and makes changes queryable within tens of seconds; this is an objective to validate, not an asserted production SLA.

Triggers default to 10 seconds, Kafka ingestion to at most 20,000 offsets per trigger and file ingestion to 32 files per trigger. Those bound intake, not total memory: one file may contain many rows, and a single parent update may affect many children. Increasing `maxOffsetsPerTrigger` trades backlog reduction for batch duration and pressure on downstream MERGE. `maxRecordsPerFile=1000000` is an upper bound and does not eliminate small files.

These are engineering starting choices exercised by the recorded workload, not the result of an exhaustive A/B tuning study. No measured optimum is claimed. The 1 GiB gap between the pipeline's 2 GiB heap and 3 GiB container cap is a local process budget, not an explicit distributed `spark.executor.memoryOverhead` setting.

| Setting | Decision and tradeoff |
|---|---|
| `local[2]` and 2-CPU container quota | Two task slots shared by both queries; leaves resources for the source and CDC. This does not create two distributed executors |
| Four shuffle partitions | A small initial task count for the local parallelism; revisit task sizes/counts as data or available cores increase |
| Delta `snapshotPartitions=2` | Small local parallelism for reconstructing Delta log state; reconsider when metadata grows |
| Ten-second triggers | Balance update frequency against job/commit overhead and small-file creation |
| 20,000 Kafka offsets per trigger | Bound intake across the subscribed partitions; does not guarantee throughput or bound downstream fanout |
| 32 Bronze files per trigger | Bound the file backlog admitted per batch; file sizes and row counts still matter |
| Persist only the current batch and decoded changes | Reuse inputs for validation/MERGE, then release them in `finally`; avoids caching the whole historical lake |
| AQE configured on the session | Inspect the effective executed plan: streaming plans disable AQE, so a global `true` alone is not proof that every `foreachBatch` join uses it |
| No physical table partitioning in the demo | Avoid excessive tiny partitions; large-table file layout must be selected from real access/update patterns |

The observed environment and benchmark sizes are known; production arrival rate, event-size distribution, update/delete mix, hot-key fanout, query concurrency, remote storage throughput and a numeric freshness SLA were not supplied. Those unknowns prevent a defensible fixed production executor count.

## Key Spark metrics used

The local resource budget was selected first; these metrics were then used to assess feasibility and identify follow-up investigations. They were not used to derive an optimal executor count or prove that the chosen heap has ample headroom. The table below is the **initial baseline**, retained separately from the [later stress metrics](performance-stress.md#key-native-spark-metrics). Native task/streaming metrics and the application's benchmark/visibility timings are distinguished: unique-event throughput and end-to-end visibility are application measurements, not executor-utilization metrics.

| Measurement | Recorded result | Interpretation |
|---|---:|---|
| Initial materialization | 600,200 events in 43.764 s | About 13,714 input events/s for this synthetic materialization phase, excluding separate initialization/Bronze write time |
| Update phase | 10,000 order updates in 13.111 s | Includes propagation to their 50,000 associated item facts in the fixture |
| Task attempts / failed attempts | 585 / 0 | Across the complete benchmark application |
| Summed executor CPU / run time | 46.869 s / 65.956 s | Totals across initialization, load, update and checks; concurrent tasks make these different from wall time |
| Task GC time, summed | 3.170 s | Per-task accounting can overlap; not a process-wide GC percentage |
| Memory spill / disk spill | 0 / 0 bytes | No task spill was recorded; peak process memory and all disk usage were not measured by these counters |
| Shuffle read / written | Approximately 66.1 MB each | Redistributed data across the application's stages |
| Task run time p50 / p95 / maximum | 19 / 517 / 4,884 ms | Inspect comparable tasks within a stage before attributing the tail to skew |
| Kafka-to-Bronze batch p50 / p95 / maximum | 2.735 / 3.158 / 3.805 s, 9 nonempty batches | Small native streaming-progress sample |
| Bronze-to-Gold batch p50 / p95 / maximum | 14.071 / 19.347 / 29.825 s, 5 nonempty batches | Includes startup/catch-up; some batches exceeded the ten-second trigger |
| Restart-followed-by-update visibility | 46.561 s upper bound | Includes pipeline restart, CLI cold starts and polling; not a steady-state p95 |

Sources: [benchmark](evidence/benchmark-100k-orders.json), [Spark tasks](evidence/benchmark-spark-tasks.json), [streaming progress](evidence/streaming-progress-summary.jsonl) and [live reconciliation](evidence/live-reconciliation.json). The progress helper selects a lower order statistic without interpolation, so its p95 on these tiny samples is an approximate sample summary, not an SLA estimate. Task input-byte totals include repeated reads and must not be interpreted as unique lake bytes or physical network traffic.

The initial observations support feasibility for that workload. The subsequent stress campaign exercised one million orders/five million items and short stepped arrival rates of 235–2,588 actual events/s, identifying pressure and backlog/freshness degradation. Neither campaign establishes sustained production capacity, a minimum viable heap, an optimal partition count or a validated production SLA.

| Sizing decision checked | Metrics used | Conclusion and next decision |
|---|---|---|
| Local two-slot / 2 GiB heap budget | Baseline completed without spill; six-million-event stress fixture recorded 846 MB disk spill and memory pressure near the container cap, without OOM | The exercised workloads completed; large backfills need their own budget. Spare capacity and minimum heap are not established |
| Ten-second triggers | Native stress materialization p95 29.825 s; growing backlog in steps above 2,000 actual events/s; within-batch freshness estimates | Trigger cadence does not guarantee freshness. The tested 30-second reference was exceeded; improve the bottleneck and validate longer loads before promising an SLA |
| Four shuffle partitions | Shuffle bytes and task durations/counts | A measured baseline exists, but no A/B comparison establishes four as optimal; compare partition counts at representative target-table size |
| Scale-out requirements | Executor CPU/run time and stage/task distribution | Useful diagnostic evidence, but no per-executor capacity or horizontal-scaling curve has been measured; production executor count remains to be validated |

## Spark cluster sizing strategy: production

**Decision status:** the delivered application has a concrete local sizing configuration. A production cluster size has not been selected or proven. The strategy below explains which variables determine that size and how it would be accepted through representative load tests. Running with `local[2]` does not validate a distributed cluster topology.

Record input rate λ (events/s), burst multiplier, average/p95 event bytes, retained table bytes and file count, changed entity distribution, children per parent (including hot keys), source snapshot size, per-stage CPU time, shuffle/spill, storage throughput and the freshness objective. Total historical row count alone does not determine the cluster.

1. Benchmark representative inserts, updates, deletes and skew against the target table size. Measure ingestion and materialization independently; replay/backfill can have a different workload mix. Find sustainable processing rate at the target batch duration, rather than extrapolating a tiny file benchmark.
2. Let `T` be the trigger interval. A first batch-input estimate is `λ × T`; include burst/catch-up capacity. A useful initial capacity margin is 30–50%, verified under load. For backlog `B` and desired recovery time `R`, required throughput is at least `λ + B/R`.
3. Estimate task CPU seconds/event from Spark event logs. Required cores are approximately `peak events/s × measured CPU seconds/event / target utilization`. This excludes I/O waits and fixed table-maintenance costs, so validate the result with batch-duration measurements. Task counts/shuffle partitions must expose enough parallelism for those cores.
4. Start a production benchmark with moderate executors (for example 4 cores, 8–16 GiB heap each, and explicitly budgeted Python/native/memory overhead). This is a benchmark starting point, not a measured requirement. Size the driver separately for plans, file metadata and concurrent queries. Choose executor count from measured throughput and then rerun the representative load.
5. Scale out when useful tasks queue and resources are saturated; scale memory or reduce task input when spill/GC is the constraint. Add Kafka partitions for an ingestion partition bottleneck, not as a universal Spark tuning measure. Changing partition counts requires preserving per-key CDC ordering. Additional cores will not fix an oversized single-key fanout, storage latency or excessive file listing.

Spark memory should cover active task working sets and any intentional cached microbatch, with headroom. The application persists only the bounded input batch and decoded changes, then unpersists in `finally`; it does not cache the entire historical lake. Data transformations and candidate joins remain distributed, without collecting business datasets to the driver. There is no unbounded stream-stream join state.

## Diagnosis and scaling actions

| Evidence | Likely constraint | Action to validate |
|---|---|---|
| Batch duration consistently exceeds trigger; lag grows | Processing cannot keep up | Identify expensive stage first; then add capacity or reduce work |
| High executor CPU and queued balanced tasks | Insufficient compute | Add executors/cores and enough task parallelism |
| Long-tail tasks / hot order IDs | Skew or parent fanout | Inspect key distribution; isolate/split hot workloads; avoid blindly broadcasting |
| High GC / memory spill | Too-large task working sets | Smaller input/tasks, more memory, narrower data and measured caching |
| High shuffle read/write | Join/window cost | Inspect plans and partition counts; broadcast only verified small dimensions |
| Large target scans per MERGE | Target layout/file skipping | Compact and cluster/Z-order on relevant keys; benchmark before/after |
| Many tiny files / driver planning time | Frequent small appends/merges | Adjust cadence/write parallelism; schedule maintenance |
| Kafka lag despite idle Spark | Source partition or broker bottleneck | Inspect broker I/O, partitions and consumer stages |
| Source replication-slot WAL growth | CDC/storage outage | Restore connector or capacity; monitor slot LSN and WAL retention |
| Fresh Gold but slow CLI | Query cold start or scans | Separate query latency from data freshness; consider an always-on serving engine |

The current incremental algorithm limits business recomputation to changed entities, but **does not guarantee constant-time lookup** in large Delta targets. Semi-joins/MERGE can scan many files. At large scale, measure those scans and use appropriate file layout, compaction, key locality and storage throughput. Do not claim that incremental source intake alone makes target lookup cheap. An order-item index/materialized access path may be justified if parent updates dominate; its consistency cost must be included.

For a large workload, start with target Parquet files in the approximate 128–256 MiB range and tune from observed task/read behavior. Avoid a global `coalesce(1)` bottleneck. Small dimensions may remain unpartitioned and be broadcast if their measured size is safe. Larger tables may use coarse partitions when their size and access patterns justify pruning; mutable date fields and high-cardinality IDs require care. Delta `OPTIMIZE`/Z-order can improve locality, but needs a benchmark and scheduled single-writer coordination. Raw Bronze files must not be rewritten underneath the active Parquet file-source checkpoint.

## Evidence and monitoring

The application persists native `StreamingQueryProgress` JSON and Spark event logs. Inspect `numInputRows`, input/processed rows per second, batch durations, Kafka start/end/latest offsets, outstanding offsets, task times, executor CPU/GC, shuffle, memory/disk spill, files scanned and Delta MERGE metrics (`DESCRIBE HISTORY`). Record end-to-end source-change visibility independently from task throughput.

`foreachBatch` performs several actions over cached data. Spark's reported input rows/rates can be inflated by repeated scans, especially MERGE; `publication.input_events` records the actual raw batch count. Do not interpret reported processedRowsPerSecond as unique-event throughput without checking that distinction.

```sh
# Synthetic run, isolated under /lake/benchmarks/run-*:
docker compose run --rm --no-deps test benchmark --orders 10000 --items-per-order 5
# Actual source reconciliation and a restart-followed-by-update:
python3 scripts/verify_live.py
# Copy native progress for offline inspection:
docker compose cp pipeline:/lake/observability/progress ./artifacts/progress
python3 scripts/progress_report.py ./artifacts/progress
```

The benchmark reports fixture volume, execution settings, Bronze write time, materialization time, update time and correctness. It is not Kafka throughput or a production capacity claim. The initial live test reports an upper bound on change visibility including pipeline restart, CLI cold starts and polling; this is not steady-state p95 latency. The later [stress report](performance-stress.md) separates ingestion bursts, materialization, within-batch freshness estimates and query cold starts. Actual results are retained in [evidence](evidence/).

Alert on sustained freshness/batch-duration breaches, growing Kafka or Bronze lag, query exceptions, quarantine rows, source WAL retention, disk usage and small-file growth. Persist logs centrally in production. Retention/compaction must preserve at least the active publication's versions, the longest reader and the maximum recovery window; automatic VACUUM is intentionally absent from this demo.
