# Stress measurements and sizing implications

This extends the [initial benchmark](performance.md#key-spark-metrics-used) with larger historical tables, stepped CDC load, ingestion bursts, parent fanout, query concurrency and broker recovery. These are actual local measurements, not a production capacity specification. The key result is that the application completed the exercised workloads correctly, while memory pressure and freshness degradation appeared before any unplanned container failure.

## Environment and method

The campaign ran on October 19–20, 2026 in America/Sao_Paulo; retained timestamps use UTC. An isolated Compose project used separate source, Kafka, Connect, lake and checkpoints. The original stack remained stopped. Spark 3.5.8 / Delta 3.3.2 ran on ARM64 in a Docker VM with approximately 7.65 GiB RAM and 10 logical CPUs available.

The pipeline retained `local[2]`, a two-CPU quota, 2 GiB heap, 3 GiB container cap, four shuffle partitions, two Delta snapshot partitions, ten-second triggers, 20,000 Kafka offsets and 32 Bronze files per trigger. Both streams shared the same task slots. PostgreSQL/Connect/Kafka retained the repository's resource limits. The isolated Bronze A/B test changed only the intake cap to 200,000 offsets for its second pair of bursts.

Test-only wrappers timed the original Silver MERGEs, Gold refresh and publication functions; business transformations were unchanged. Streaming instrumentation added one aggregation per batch over the cached input to estimate source timestamp quantiles. That overhead is included and recorded separately in [batch measurements](evidence/stress/streaming-batches.json). Native streaming progress and task metrics were retained independently.

Each scenario ran once. CDC steps lasted approximately two minutes, with backlog drained between steps. The source generator committed batches of orders and five items per order, nominally once per second, pausing for administrative sampling. Actual arrival rates include SQL time and those pauses. A 30-second freshness reference was chosen for this experiment; the assessment supplies no numeric SLA. This is a short load campaign, not a long-duration soak or distributed scale-out test.

## Historical size and materialization cost

Each uniform fixture contains 100 customers, 100 products and five items per order. After initial materialization, 10% of orders receive status updates that propagate to their stored children. Timings exclude separate Spark initialization, Bronze fixture writing and subsequent verification.

| Orders / items | Initial events | Materialization | Silver MERGEs | Gold refresh | Parent updates | Update duration | Disk spill¹ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100,000 / 500,000 | 600,200 | 40.6 s | 11.3 s | 22.5 s | 10,000 | 12.6 s | 0 MB |
| 500,000 / 2,500,000 | 3,000,200 | 139.2 s | 29.6 s | 84.6 s | 50,000 | 24.8 s | 137.7 MB |
| 1,000,000 / 5,000,000 | 6,000,200 | 370.5 s | 79.8 s | 232.5 s | 100,000 | 49.6 s | 846.0 MB |

¹ Decimal MB; summed task spill across the complete application, including updates/checks. Silver/Gold columns omit validation and publication, so they do not sum to the whole materialization. All fixtures passed their expected counts. [Measurements](evidence/stress/materialization.json).

Gold consumed approximately 63% of initial materialization at the largest size: item-fact MERGE took 149.0 s and date-dimension MERGE 51.0 s. Doubling from approximately three to six million input events increased duration by 2.66×. Incremental input does not eliminate the cost of scanning and merging large targets.

### Key native Spark metrics

| Application | Task attempts / failures | Summed CPU | Summed task run time | Summed task GC | Memory / disk spill | Task run p95 |
|---|---:|---:|---:|---:|---:|---:|
| 100k orders | 642 / 0 | 47.0 s | 65.0 s | 2.9 s | 0 / 0 MB | 0.497 s |
| 500k orders | 689 / 0 | 196.3 s | 258.1 s | 12.1 s | 973.1 / 137.7 MB | 1.631 s |
| 1m orders | 680 / 0 | 482.8 s | 755.9 s | 51.3 s | 4,513.1 / 846.0 MB | 6.414 s |

These are task sums across complete applications, not wall time or process-wide GC percentages. Memory spill is the estimated deserialized size of spilled data. Global task p95 mixes stages and does not independently prove skew. Source event-log extracts: [100k](evidence/stress/scale-100k-spark-metrics.json), [500k](evidence/stress/scale-500k-spark-metrics.json), [1m](evidence/stress/scale-1m-spark-metrics.json).

During the largest fixture, the [cgroup probe](evidence/stress/cgroup-scale-1m.json) recorded a memory peak approximately at the 3 GiB limit, 673 memory-limit events and zero OOM/OOM kills. Docker's sampled maximum was approximately 2.73 GiB; its indicator discounts some cache. CPU-quota throttling was observed. This is evidence of pressure, not ample memory headroom. No Docker resource sample was retained for the 100k run.

A separate fixture used one order with one million items. Its initial 1,000,201 events materialized in 50.1 s. Updating **one parent** took 14.1 s, including 10.6 s in Gold and 7.2 s in the item-fact MERGE. Counts/status propagation passed, with no recorded task spill. This demonstrates fanout cost; it does not isolate executor skew. [Measurements](evidence/stress/materialization.json), [tasks](evidence/stress/hot-1m-items-spark-metrics.json).

## Kafka → Bronze intake experiment

An independent two-partition topic received synthetic payloads from the Kafka performance producer, with `acks=all` and no producer compression. Silver/Gold did not run in this phase. The checkpoint was preserved when increasing the offset cap.

| Payload bytes | Burst events | Offsets / trigger | Observed time until drained |
|---|---:|---:|---:|
| 500 | 250,000 | 20,000 | 127.8 s |
| 5,000 | 100,000 | 20,000 | 57.6 s |
| 500 | 250,000 | 200,000 | 20.9 s |
| 5,000 | 100,000 | 200,000 | 20.4 s |

Times include producing and waiting for monitoring to observe completion. The original cap admits roughly 2,000 offsets/s at ten-second triggers; raising it removed that admission constraint for these bursts. Physical Parquet verification found 700,000 rows and 700,000 distinct Kafka coordinates after checkpoint resumption. [Burst measurements](evidence/stress/bronze-load-levels.jsonl), [physical verification](evidence/stress/bronze-integrity.json).

These synthetic payloads do not model the structure/compressibility of actual Debezium events. Local page cache and short bursts preclude a sustainable broker-capacity claim. Increasing offsets does not resolve downstream Gold cost or bound parent fanout.

## Full PostgreSQL → Debezium → Gold load

| Nominal events/s | Actual events/s | Emitted events | Kafka→Bronze backlog at load end | Bronze→Gold backlog at load end | Additional drain time |
|---|---:|---:|---:|---:|---:|
| 300 | 235.3 | 29,100 | 1,200 | 2,700 | 29.8 s |
| 1,200 | 954.2 | 116,400 | 0 | 28,800 | 16.7 s |
| 3,000 | 2,356.4 | 285,000 | 56,020 | 99,993 | 66.1 s |
| 6,000 | 2,587.5 | 312,000 | 86,022 | 59,993 | 69.8 s |

The highest nominal rate was **not achieved**. Interpret actual rates, not the labels. Backlog counters are sampled near each other, not transactionally across all services. Raw publication event counts are used because native `foreachBatch` input-row counts may include repeated scans. [Rates, timestamps, counters and drains](evidence/stress/streaming-levels.json).

| Actual events/s | Non-snapshot batches | Median of within-batch p95 ages | Largest within-batch p95 age |
|---|---:|---:|---:|
| 235.3 | 12 | 25.9 s | 32.5 s |
| 954.2 | 8 | 34.1 s | 41.7 s |
| 2,356.4 | 8 | 63.7 s | 69.9 s |
| 2,587.5 | 7 | 74.7 s | 112.5 s |

Age is publication completion minus Debezium `source.ts_ms`, excluding snapshot events. Each batch's p95 age is estimated from its timestamp fifth percentile; the median/maximum of those values **is not a global event p95**. Batches are assigned using their minimum source timestamp; a boundary batch can contain adjacent arrivals. These small samples do not certify an SLA. Even the lowest rate had a batch p95 above the 30-second reference.

Native progress over the whole campaign records Kafka→Bronze batch p50/p95/max of **0.848/1.447/2.610 s** across 62 nonempty batches, and Bronze→Gold **15.007/29.825/33.262 s** across 40 batches. The helper uses a lower order statistic without interpolation. Batch execution duration excludes earlier queuing. [Native progress](evidence/stress/online-progress-summary.jsonl).

The streaming application's event log records **11,487 task attempts, zero failures**, summed CPU **337.4 s**, task GC **22.7 s**, and zero task spill. The small bounded streaming batches differ materially from the oversized offline fixture. [Task extract](evidence/stress/online-spark-metrics.json).

## Correctness, recovery and serving

All four assessment metrics matched independent PostgreSQL queries after load, after **12,375 parent updates and 6,378 item deletes**, and after broker recovery. Delete tombstones were retained in raw event counts. The final physical check found **774,001 Bronze rows and distinct Kafka coordinates**, matching topic offsets and successfully materialized input events. Additional source/Gold aggregates matched **124,756 orders**, **12,378 completed orders**, **617,528 active items**, and **3,167,520 units**. [Three metric reconciliations](evidence/stress/reconciliation.jsonl), [final integrity](evidence/stress/final-integrity.json).

The source accepted **6,000 events while the isolated broker was stopped**. The supervisor restarted Kafka; all backlog drained and reconciliation passed. The observed interval from beginning the stop operation to observing recovery was **53.5 s**, including stopping, waiting, starting, processing and polling. Spark did not require a restart. This tests a temporary outage with persistent volumes; a single broker is still not highly available and disk loss was not tested. [Recovery record](evidence/stress/broker-outage.jsonl).

The initial 185-row snapshot was emitted twice. Business-state reconciliation passed despite this replay; raw Bronze preserves both sets of coordinates. The campaign's planned counter was therefore 185 below the actual final offset total. Final independent checks used the actual offsets and physical rows, not that planned counter.

One, two and four simultaneous fresh query containers completed in **24.6, 37.9 and 65.2 s** respectively, without failures/OOM. Each ran the four metrics plus two integrity aggregates; group times include cold start and supervision. Queries ran with the source quiet and the pipeline still active, not during peak writes. [Concurrency records](evidence/stress/query-concurrency.jsonl), [query timings](evidence/stress/query-durations.json), [actual result including publication](evidence/stress/query-output.json).

## Implications for sizing

- The current heap/CPU budget completed the tested workloads, but large backfills reached memory pressure and spill. It is not a proven production cluster or a resource optimum.
- Gold was the largest measured materialization cost. Benchmark target-table layout/compaction, item-fact access and date-dimension work before assuming extra Kafka partitions solve the bottleneck.
- Separate admission, processing and freshness budgets. Raising offsets can reduce Kafka lag while increasing downstream batch size and latency. Small batches repeatedly pay planning, validation, MERGE and publication costs; bulk events/s cannot be reused as a streaming SLA.
- Account for historical target size, event bytes, mutation mix and children per changed parent. One event can refresh a million items.
- Size catch-up capacity using arrival rate plus backlog divided by the required recovery window. Compare memory, cores and shuffle partitions with representative long-running loads and shared durable storage.
- CLI cold start and contention are separate from data freshness. Frequent interactive queries warrant evaluating a persistent serving process.

Independent saturation limits for PostgreSQL, Debezium and broker disk were not found. Neither horizontal scaling, a minimum viable heap, a maximum user count nor a multi-hour freshness target was established. The original services and isolated campaign were stopped afterward, preserving volumes.

The retained files are curated exports from completed executions; campaign-specific instrumentation and large raw logs are not part of this repository. The root README's benchmark reproduces the core synthetic workload shape, not this entire historical campaign or its exact timing.
