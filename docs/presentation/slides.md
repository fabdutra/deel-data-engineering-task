# Presentation outline and English speaker notes

## 01. ACME Delivery: streaming operational analytics

Open with the operational problem: ACME needs trustworthy order visibility as demand grows. I built and tested the complete source-to-query path. I will explain the consistency contract first, then the measured performance constraints. The main narrative is approximately 18–20 minutes; the appendix supports technical questions. No business ROI or production SLA is claimed.

Sources: [README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/README.md), [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md)

## 02. Four questions drive the platform

Tie each output to an operational decision. Order counts must not be multiplied by joining to item rows. Pending items expose line count and total units because the wording can otherwise be ambiguous. The output is an operational current-state view, not a claimed business-impact measurement.

Sources: [sql/open-orders.sql](https://github.com/fabdutra/deel-data-engineering-task/blob/main/sql/open-orders.sql), [sql/pending-items.sql](https://github.com/fabdutra/deel-data-engineering-task/blob/main/sql/pending-items.sql)

## 03. Make the assumptions explicit

State the interpretation before showing results. History is the initial snapshot plus persisted state from earlier batches, not SCD2 or every prior order status. The source fixture is synthetic and continuously updated. The PDF asks for low latency without a numeric SLA; 30 seconds is my experimental reference. Production arrival rates and query-concurrency targets were not supplied.

Sources: [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md), [docs/architecture.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/architecture.md)

## 04. One complete source-to-query path

Walk left to right: pg_cron changes PostgreSQL; Debezium snapshots and then tails WAL; Kafka carries four entity topics. Spark has two streams and two persistent checkpoints: Kafka to raw Parquet, then Bronze to foreachBatch. Silver stores ordered current state. Gold joins that historical state and writes facts/dimensions. Only a completed manifest is exposed through the separate Spark SQL CLI. Invalid events block publication and go to quarantine. Both streams share two local slots and one materializer owns the mutable tables. Secrets come from the ignored environment/provider; connector initialization is one-off.

Sources: [docs/architecture.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/architecture.md), [docker-compose.yaml](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docker-compose.yaml)

## 05. Two fact grains prevent double counting

The order fact is one row per order and is the source for order-count questions. The item fact is one row per item and carries the current parent status, date and customer for filtering. Dimensions are SCD1 with stable source identifiers, not a surrogate-key service. Dates have an unknown key of zero. Parent changes refresh historical children. Customers/products missing from dimensions do not drop operational order facts.

Sources: [app/gold.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/gold.py), [docs/architecture.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/architecture.md)

## 06. Every microbatch earns its publication

Bronze retains the exact Kafka key/value bytes and coordinates, including tombstones. The second stream validates before mutating current-state tables, selects the latest event by LSN/offset and conditionally merges Silver. Gold recomputes affected entities by joining the stored state. Only after all writes succeed is a single Delta manifest published. Most transformations use PySpark DataFrames and Delta merge; serving queries are Spark SQL. The pipeline does not collect business datasets to the driver.

Sources: [app/materialize.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/materialize.py), [app/storage.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/storage.py)

## 07. Historical joins are part of the stream

Use this illustrative sequence to explain the integration test pattern, not as a captured source record. First an order enters by snapshot. Later a new item arrives with no order event in that batch; its parent status/customer/date come from Silver. Later the order completes with no new item events; stored items are refreshed from the parent change. This is why a watermark-based stream-stream window join would be the wrong contract for long-lived operational orders.

Sources: [app/gold.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/gold.py), [docs/evidence/historical-join-tests.log](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/historical-join-tests.log)

## 08. Choose the simplest components that fit

Explain the choices as tradeoffs. Parquet Bronze is append-only raw event preservation; Delta is reserved for mutable current state and dimensional tables. Native checkpoints/metadata must remain intact. The publication manifest provides consistent supported reads but not global source transaction buffering. CLI is an allowed assessment serving option, with a documented cold-start/concurrency cost. Avoid presenting the local plaintext network or a single Kafka broker as production security or availability.

Sources: [docs/architecture.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/architecture.md), [README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/README.md)

## 09. Size for working sets and recovery

Be precise: local[2] is two task slots sharing one driver JVM, not two executors. Heap is 2 GiB and the container cap is 3 GiB. Four shuffle partitions are a measured starting configuration, not an optimum from an exhaustive A/B study. Both streams compete for these slots. Variables include actual event rate/bytes, historical target size, mutation mix, parent fanout, file layout and desired recovery/freshness. Production must separately size executors and driver on shared durable storage.

Sources: [docs/performance.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance.md), [docker-compose.yaml](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docker-compose.yaml)

## 10. Triangulate correctness and performance

Separate evidence types. Real Spark/Delta integration tests cover ordering, duplicate events, deletes, late parents, invalid events and crash/replay. PostgreSQL is an independent oracle for all four business metrics. Bulk materialization is measured independently from Kafka/Bronze and full CDC load. Physical checks compare Kafka coordinates and published input counts. There is one execution per stress scenario and approximately two-minute arrival steps, not a statistically certified soak. Captured result files are actual outputs, not fabricated screenshots.

Sources: [tests/test_pipeline.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/tests/test_pipeline.py), [docs/evidence/README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/README.md)

## 11. Gold dominates as retained state grows

The initial materialization grows from 40.6 to 139.2 to 370.5 seconds across roughly 0.6, 3 and 6 million events. Timings exclude Spark initialization, separate Bronze writing and later verification. Gold takes 232.5 seconds, approximately 63% of the largest initial load. Task disk spill is 846 MB across the complete largest application, not just this plotted initial phase. The cgroup probe reaches approximately its cap without OOM. Do not reuse bulk events/s as a streaming capacity promise.

Sources: [docs/evidence/stress/materialization.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/materialization.json), [docs/evidence/stress/scale-1m-spark-metrics.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/scale-1m-spark-metrics.json)

## 12. Freshness degrades without container failures

The x-axis uses measured arrival rates: 235, 954, 2356 and 2587 events/s. The nominal 6000 events/s target was not achieved. Each age p95 is computed within a batch from source timestamps, excluding snapshot. The plotted median/maximum of batch p95 values is not a global event p95. Above about 2000 events/s, the original intake cap contributes growing lag. Even the smallest stage has a batch p95 above the chosen 30-second reference. Backlogs drain after load stops, but that is different from preserving freshness while arrivals continue.

Sources: [docs/evidence/stress/streaming-levels.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/streaming-levels.json), [docs/evidence/stress/streaming-batches.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/streaming-batches.json)

## 13. Correctness survives a temporary broker outage

Three independent comparisons of all four business metrics passed: after load, after 12375 parent updates and 6378 item deletes, and after broker recovery. Final Kafka offsets, raw Bronze rows/unique coordinates and successfully published raw input counts all agree at 774001. These are event coordinates, not unique business entities. The broker was stopped, 6000 source events committed, and the supervisor restarted Kafka. 53.5 seconds includes stop/wait/start/process/poll; Spark did not need a restart. This proves temporary outage recovery with retained volumes, not HA or disk-loss recovery.

Sources: [docs/evidence/stress/final-integrity.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/final-integrity.json), [docs/evidence/stress/broker-outage.jsonl](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/broker-outage.jsonl)

## 14. Scale the measured constraint first

Separate implemented behavior from proposed production changes. First measure retained-target scans and Gold merge/layout costs, including the expensive date-dimension work. Then compare memory, task sizes and partitions under a defined freshness objective. Decouple ingestion/materialization resources when competition is the constraint. Production needs object/shared storage and replicated Kafka with security/monitoring. A single parent updating a million children took 14.1 seconds, illustrating why offsets alone do not size work. No claimed fixed executor count follows from this local run.

Sources: [docs/performance-stress.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance-stress.md), [docs/performance.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance.md)

## 15. Inspect, reproduce and discuss

Close the core narrative with a clear handover. The repository contains application code, SQL, Docker, tests, measured evidence, documentation and the source for this presentation. The standard source creates and mutates data automatically. During a demo, wait for the published-batch log before querying and point out the manifest timestamp. Distinguish data freshness from a fresh JVM cold start. The optional technical appendix follows. Do not run benchmarks alongside the demo on the small VM.

Sources: [README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/README.md), [docs/evidence/README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/README.md)

## 16. Upserts and deletes preserve ordering

Silver orders every entity by primary key plus PostgreSQL LSN and Kafka offset. The per-key partition-ordering assumption must be preserved during topic changes. Deletes use logical markers that retain ordering so stale updates cannot resurrect data. Kafka null-value tombstones are preserved in Bronze and do not delete a second time. Customer/product dimensions are SCD1; facts exclude logical deletions in supported queries. Date members are merged by date key and retained. Missing parents are retained in Silver and repaired when the parent arrives.

Sources: [app/cdc.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/cdc.py), [app/storage.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/storage.py)

## 17. Publish a complete read boundary

Each Delta transaction is per table. The manifest is the cross-table read contract: capture it once and use versionAsOf for each Gold table. If a batch fails after Silver or some Gold writes, the old manifest remains readable and replay converges from authoritative Silver. After publication, readers can see the new complete versions. Quarantine blocks the batch until parser/validation is corrected; a later source correction does not skip an invalid retained event. Kafka failOnDataLoss is true and source transactions may be split across topic batches.

Sources: [app/materialize.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/materialize.py), [app/metrics.py](https://github.com/fabdutra/deel-data-engineering-task/blob/main/app/metrics.py)

## 18. Intake and serving have different limits

Ingestion uses a separate two-partition synthetic topic and no Gold work. With the same checkpoint/resources, a 250000-record, 500-byte burst drains in 127.8 seconds at 20000 offsets and 20.9 seconds at 200000 offsets. Payloads are synthetic and do not guarantee realistic compression, so this is not broker disk saturation. Query concurrency tests use fresh separate JVMs, a quiet source and a live pipeline; each executes four metrics plus two checks. They show cold-start/contention cost, not peak interactive throughput under maximum writes.

Sources: [docs/evidence/stress/bronze-load-levels.jsonl](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/bronze-load-levels.jsonl), [docs/evidence/stress/query-concurrency.jsonl](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/query-concurrency.jsonl)

## 19. Native metrics support the sizing discussion

Read these as summed task measurements across each complete application. CPU/run time can exceed wall time, and task GC accounting can overlap. Memory spill is an estimated deserialized volume, not extra physical RAM. The table mixes stages, so its global task p95 is not proof of skew. The streaming run had 11487 task attempts with no failures and no task spill; that does not contradict the large offline fixture spilling because its admitted working set is very different. The cgroup sample records 673 memory-limit events and no OOM.

Sources: [docs/evidence/stress/scale-1m-spark-metrics.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/scale-1m-spark-metrics.json), [docs/evidence/stress/online-spark-metrics.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/online-spark-metrics.json)

## 20. Captured business output is inspectable

These values are selected from the retained actual query JSON, not a screenshot or a simulated live UI. The publication is batch 39 with pinned Gold versions. Show one row from each of the four metrics; the complete result remains linked. This synthetic load deliberately concentrates new orders on customer 1 and product 1, so the top-customer/product distribution is not representative production demand. All four metrics were compared independently with PostgreSQL.

Sources: [docs/evidence/stress/query-output.json](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/query-output.json), [docs/evidence/stress/reconciliation.jsonl](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/stress/reconciliation.jsonl)

## 21. Limits are part of the analytical result

Be ready to explain the gaps without overclaiming. This is a complete assessment implementation, not production certification. Historical semantics, atomicity and single-writer constraints are deliberate. Arrival-rate steps are short and pauses affect actual rates; the nominal 6000 target was not attained. Batch age quantiles are not event-global p95. Snapshot replay emitted the initial 185 records twice; raw coordinates and business reconciliation still matched. A minimum heap, optimum partitions or independently saturated source/broker capacity were not established.

Sources: [docs/performance-stress.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/performance-stress.md), [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md)

## 22. Every claim has a path back to the work

Finish technical discussion by pointing to the reviewer index in README. All underlying implementation and retained evidence are in the private repository. This presentation has a reproducible source generator, pinned dependencies, exported outline/English speaker notes and a SHA-256 manifest of inputs/outputs. The private Portuguese study guide is intentionally not part of the submission. Rebuilding the deck uses existing evidence and does not start Docker or rerun the historical performance campaign.

Sources: [docs/assessment-deliverables.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/assessment-deliverables.md), [docs/evidence/README.md](https://github.com/fabdutra/deel-data-engineering-task/blob/main/docs/evidence/README.md)
