# Presentation outline and English speaker notes

## 01. ACME Delivery: streaming operational analytics

Present ten slides in about 10–12 minutes, with no appendix. Explain the architecture, deliverables and measured performance. ACME needs current order information for operational planning. This is a working assessment implementation, not a certified production cluster.

Sources: [README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/README.md), [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md)

## 02. What was delivered

The CLI answers all four business questions. Pending items expose both line count and total units. The repository also contains the architecture, sizing strategy, application, SQL, tests, experiment code and recorded evidence. No business ROI is claimed from synthetic data.

Sources: [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md), [sql/pending-items.sql](https://github.com/fabdutra/deel-data-engineering-task/blob/main/sql/pending-items.sql)

## 03. How the components connect

Debezium loads the initial snapshot and captures PostgreSQL WAL changes. Kafka has four entity topics. Spark has two streams and checkpoints: Kafka to Parquet Bronze, then Bronze to foreachBatch for Silver/Gold. Both streams share two local slots. One materializer owns the tables. A completed version manifest is the supported read boundary. Invalid events go to quarantine and block publication. Secrets use the environment/provider. Local plaintext traffic and a single broker are not production TLS/HA.

Sources: [docs/architecture.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/architecture.md), [docker-compose.yaml](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docker-compose.yaml)

## 04. How data becomes useful

Bronze preserves Kafka bytes and coordinates. Silver orders changes using PostgreSQL LSN and Kafka offset, conditionally merges the latest key state and retains delete guards. A new item joins a stored order; a changed parent refreshes stored children even without item events. Gold separates order/item fact grains and customer/product/date dimensions. Dimensions are SCD1. History means snapshot plus accumulated current state, not SCD2. The CLI reads the completed versions from a single manifest; transactions split across topics are eventually reconciled.

Sources: [app/gold.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/gold.py), [app/materialize.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/materialize.py)

## 05. The local Spark size and its limits

local[2] is two task slots in one driver JVM, not two executors. Both streams share them. Heap is 2 GiB within a 3 GiB container budget. Four shuffle partitions are an initial setting, not a proven optimum. Kafka intake is 20000 offsets per ten-second trigger; Bronze intake is 32 files per batch. These do not bound fanout or retained-target scans. Production needs actual rates/bytes, mutation mix, historical targets, storage, fanout and freshness/recovery goals. Shared durable storage and distributed measurements are still required.

Sources: [docs/performance.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance.md), [docker-compose.yaml](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docker-compose.yaml)

## 06. Which performance metrics were measured

Actual arrival rate includes SQL execution and sampling pauses. Batch duration is active processing; freshness includes previous waiting. Each age p95 is calculated inside one batch. Its median/maximum across batches is not a global event p95. Native Spark metrics include CPU, task duration, GC, shuffle, spill and failures. Each stress scenario ran once, with arrival steps of about two minutes. Thirty seconds is an experimental reference, not a numeric SLA supplied by Deel.

Sources: [docs/performance-stress.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance-stress.md), [docs/evidence/stress/streaming-levels.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/streaming-levels.json)

## 07. Large loads: Gold takes most of the time

Initial fixtures contain about 0.6, 3 and 6 million events. Materialization excludes Spark initialization, separate Bronze writing and later checks. The largest took 370.5 seconds; Gold used 232.5 seconds, about 63%. Task disk spill of 846 MB, CPU of 482.8s and GC of 51.3s are totals for the whole largest application, including update/checks. Task sums are not wall time or JVM-wide GC percentages. Failed attempts were zero. The memory probe approached the container cap without OOM. Bulk events/s does not certify streaming freshness.

Sources: [docs/evidence/stress/materialization.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/materialization.json), [docs/evidence/stress/scale-1m-spark-metrics.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/scale-1m-spark-metrics.json)

## 08. Streaming: more input means more delay

Use measured input rates: 235, 954, 2356 and 2587 events/s. The nominal 6000/s target was not reached. The chart shows median/highest within-batch p95 ages, not global event p95. Above roughly 2000/s the original cap contributes growing lag. Even the lowest step had a batch p95 above the 30s experimental reference. Highest batch p95 was 112.5s. All steps drained after input stopped; the highest two needed about 66s and 70s. These short tests do not certify a production SLA.

Sources: [docs/evidence/stress/streaming-levels.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/streaming-levels.json), [docs/evidence/stress/streaming-batches.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/streaming-batches.json)

## 09. Correctness and recovery were checked

774001 counts raw events, not unique orders. Topic offsets, physical Bronze rows/distinct Kafka coordinates and published input counts matched. Four business metrics matched independent PostgreSQL queries three times: after load, after mutations and after recovery. We applied 12375 order updates and 6378 item deletes. While Kafka was stopped, 6000 events committed to the source. The 53.5s interval includes stop, wait, restart, processing and polling. Spark needed no restart. This demonstrates recovery with retained volumes, not single-broker HA or disk-loss recovery.

Sources: [docs/evidence/stress/final-integrity.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/final-integrity.json), [docs/evidence/stress/broker-outage.jsonl](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/broker-outage.jsonl)

## 10. Delivery summary and next step

Close with a working source-to-query platform, measured evidence and a clear next experiment. Prioritize Gold target scans/merge work, then compare resources and file layout using real arrival/freshness targets. Repository contents include application, SQL, Docker, tests, experiment code and slide sources. The CLI reads completed publications. The README indexes the deliverables. Detailed explanations remain in documentation and speaker notes; there is no slide appendix.

Sources: [README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/README.md), [docs/presentation/README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/presentation/README.md)
