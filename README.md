# ACME Delivery: streaming operational analytics

A Dockerized PySpark Structured Streaming application that combines the source database's initial snapshot with ongoing Debezium CDC, maintains current state in Delta, and serves four operational metrics through a CLI. Bronze preserves the original Kafka event bytes in append-only Parquet.

This repository preserves the complete history of [Deel's starter repository](https://github.com/juan-domingos-deel/deel-data-engineering-task). A private independent copy is used because GitHub does not allow a private fork of a public repository.

## Reviewer guide: deliverables and evidence

Start with the [assessment requirements mapped to the implementation](docs/assessment-deliverables.md). The table below links each deliverable to its documentation, implementation or recorded execution evidence.

| Deliverable / review topic | Where to look | What to inspect |
|---|---|---|
| Run the Dockerized solution | [Run instructions](#run), [Compose stack](docker-compose.yaml) | Setup, startup, container dependencies and commands to query the metrics |
| Spark Structured Streaming and historical-data joins | [Historical-data join contract](docs/architecture.md#joining-cdc-with-historical-data), [pipeline](app/pipeline.py), [Gold processing](app/gold.py) | Debezium snapshot + CDC, accumulated Silver state and joins with records absent from the current batch |
| Consistent dimensional output | [Architecture and data model](docs/architecture.md), [business definitions](#business-definitions-and-data-model) | Fact/dimension grains, Delta tables, status propagation and publication consistency |
| Architecture diagram | [Component diagram](docs/architecture.md#architecture-and-data-contracts) | Source, Debezium, Kafka, Spark, medallion storage, CLI and observability |
| Spark cluster sizing strategy and decision variables | [Local sizing](docs/performance.md#spark-cluster-sizing-strategy-local-deployment), [production sizing strategy](docs/performance.md#spark-cluster-sizing-strategy-production) | CPU/heap/container budgets, intake limits, workload assumptions and scaling decisions |
| Key Spark metrics | [Initial metrics and interpretation](docs/performance.md#key-spark-metrics-used), [stress task metrics](docs/performance-stress.md#key-native-spark-metrics), [native streaming progress](docs/evidence/stress/online-progress-summary.jsonl) | CPU, task durations/failures, GC, shuffle, spill and streaming batch duration |
| Four business queries and CLI access | [CLI commands](#run), [SQL definitions](sql/), [recorded CLI output](docs/evidence/cli-metrics.txt), [stress query results](docs/evidence/stress/query-output.json) | Open orders, top delivery dates, pending items and top customers, with publication metadata |
| Evidence of the application running | [Evidence index](docs/evidence/README.md), [integration execution](docs/evidence/integration-tests.log), [historical-join execution](docs/evidence/historical-join-tests.log) | Actual test logs, CLI/query results, source reconciliation and native metrics; no Spark UI screenshots are included |
| Performance, limits and recovery under load | [Stress report](docs/performance-stress.md), [recorded results](docs/evidence/stress/) | Million-order fixture, intake experiment, freshness/backlog, broker recovery and query concurrency |
| Data correctness and reconciliation | [Correctness and recovery contract](#correctness-and-recovery), [three stress reconciliations](docs/evidence/stress/reconciliation.jsonl), [final integrity checks](docs/evidence/stress/final-integrity.json) | Four metrics compared with PostgreSQL, raw-event uniqueness, final counts and replay/delete behavior |
| Validation, operations and security | [Validation commands](#validation-and-evidence), [operations and security](#operations-and-security) | Repeatable integration/benchmark commands, checkpoints, recovery, credentials and local deployment boundaries |

## Run

Requirements: Docker Desktop with approximately **8 GB assigned to its Linux VM**, Docker Compose v2, Python 3 for credential setup, and internet access during the initial build. Images support ARM64 and AMD64. No local Spark or Java installation is required.

```sh
python3 scripts/setup.py
docker compose up -d --build
# Wait for the first 'published batch=...' log entry:
docker compose logs -f pipeline
```

Initial builds download Spark/Kafka/Delta artifacts and can take several minutes. Connector initialization waits for a healthy database, Kafka, Connect and a running connector task. The source creates its initial records and automatically changes them using pg_cron. You do not need to generate input data.

```sh
docker compose run --rm --no-deps query metrics all
docker compose run --rm --no-deps query metrics open-orders
docker compose run --rm --no-deps query metrics top-delivery-dates
docker compose run --rm --no-deps query metrics pending-items
docker compose run --rm --no-deps query metrics top-customers
# Export every result as JSON Lines; Spark logs go to stderr:
docker compose run --rm --no-deps query metrics all --format jsonl > metrics.jsonl
```

Each invocation pins every Gold table to the versions in one published manifest. The output includes the publication timestamp and batch ID. Before the first successful publication, the CLI fails with an explicit message rather than returning empty business results. Table output displays at most 100 rows by default (`--limit` changes this); JSON Lines exports all rows using an iterator. A new invocation reads the latest publication. Spark cold start adds seconds to each CLI call; this is an on-demand operational interface, not a high-QPS query server.

A metrics command never creates infrastructure. `docker compose up` starts the four source/CDC services and the pipeline; query/test/history are optional profiles. Stop without deleting data using `docker compose down`. Restarting preserves Kafka data, PostgreSQL, Bronze, checkpoints, Delta tables and Spark event logs in named volumes.

## Business definitions and data model

- Open/pending orders have status `PENDING`, `PROCESSING` or `REPROCESSING`. `COMPLETED` orders are excluded. An unknown or null status is quarantined and blocks publication.
- `fact_order` has one row per order. Counts never join to order items, which prevents fanout.
- `fact_order_item` has one row per order item. Pending items expose both `pending_item_lines` (number of rows) and `pending_units` (sum of quantities). The starter's misspelled `quanity` becomes `quantity` in Silver.
- `dim_customer`, `dim_product` and `dim_date` form the dimensional model. Stable source IDs are reused as dimension keys; customer and product attributes follow SCD1 semantics. Prices are `DECIMAL(20,4)`; incompatible values stop the pipeline. Missing dates use date key `0`. Missing or deleted customer/product dimensions do not remove operational facts from metrics.
- Top-three results return up to three rows, with date/ID as a deterministic tiebreaker.
- History means **all preexisting orders plus ongoing current state**, with CDC events retained for replay/audit. This is not an SCD2 or historical state-transition reporting product. No history preceding the initial Debezium snapshot can be reconstructed.

The four query definitions are in [sql/](sql/). See the [deliverable-by-deliverable review](docs/assessment-deliverables.md), [architecture and historical-data joins](docs/architecture.md), and [cluster sizing and performance strategy](docs/performance.md).

## Correctness and recovery

Kafka → Bronze uses Spark's native Parquet streaming sink with its metadata log and persistent checkpoint. Bronze stores binary key/value, topic, partition, offset, Kafka timestamp and ingestion timestamp; null-value tombstones are preserved. Debezium parsing happens only downstream. There is no streaming state-store join and no watermark that discards late CDC updates.

Silver keeps the latest event per primary key, ordered by PostgreSQL LSN and then Kafka offset, and uses conditional Delta MERGE. Soft-delete guards retain ordering information so an older update cannot resurrect a deleted key. Business queries exclude deleted facts. A Kafka tombstone following a delete does not delete twice.

Gold refreshes affected orders/items rather than overwriting the complete analytical dataset. Parent status/date/customer changes refresh their children. An item received before its parent is retained and excluded from metrics until the parent arrives. Invalid business events are persisted to a deduplicated quarantine table and fail the batch; the previous publication remains readable.

All Gold writes complete before a single Delta transaction updates the publication manifest. After a crash, reexecution converges to the same business state; the CLI continues to use the previous complete publication until recovery finishes. Delta transactions are per table: this manifest is the supported read interface for cross-table consistency. Direct reads of the newest individual tables bypass that contract. The system converges across topics and does not implement atomic buffering of whole source transactions.

To recover, correct the failure and restart `pipeline` with the same volumes/checkpoints. For a quarantined schema/contract error, update the parser/validation and replay the retained batch; a later source correction alone does not skip the bad event. Do not delete checkpoints, change topic subscriptions or change source identity while reusing existing tables. A full rebuild requires a separate empty lake/checkpoint location and enough Kafka retention or a controlled new snapshot. Kafka retention is seven days; missing offsets fail visibly (`failOnDataLoss=true`). Bronze currently has no automatic retention or compaction. Do not rewrite raw Parquet under an active file-stream checkpoint.

## Validation and evidence

```sh
# Runs real Spark/Delta tests, in isolated temporary tables:
docker compose run --rm --no-deps test

# Live source/Gold reconciliation, restart and subsequent CDC update:
python3 scripts/verify_live.py

# Synthetic materialization benchmark, isolated from the live analytical tables:
docker compose run --rm --no-deps test benchmark --orders 10000 --items-per-order 5
# Larger optional workload; use adequate RAM and disk:
docker compose run --rm --no-deps test benchmark --orders 1000000 --items-per-order 5
```

`verify_live.py` temporarily pauses this assessment's synthetic cron jobs, compares all four metrics, restarts the pipeline and changes an order to test recovery. It restores the order status and cron settings in `finally`. Run only against this local synthetic source. The benchmark generates fixtures on Spark executors and records separate initialization, Bronze write, initial materialization and update durations. It does **not** measure Kafka end-to-end throughput. Do not run heavy tests/benchmarks concurrently when collecting representative performance evidence.

The integration scenario covers snapshot + CDC, all four metrics, duplicate deliveries, event ordering, quantity changes, parent status propagation, crash before publication, replay, key-only deletes, stale resurrection attempts, late parents, unknown dimensions, null dates, tombstones, invalid-event quarantine and the separate Spark sessions used by `foreachBatch`.

[Recorded evidence](docs/evidence/) includes actual CLI/query output, Spark/Delta test logs, source-to-Gold reconciliations and native metrics. The [stress report](docs/performance-stress.md) extends this to one million orders/five million items, stepped CDC load, query concurrency and temporary broker outage recovery, with explicit memory/freshness limits. These are execution artifacts; Spark UI screenshots are not included. CI builds the same image and runs the integration scenario.

## Operations and security

```sh
docker compose ps -a
docker compose logs --tail 100 pipeline kafka-connect
docker compose restart pipeline
# Live Spark UI: http://localhost:4040
# Optional persisted Spark UI after jobs finish:
docker compose --profile observability up -d history
# History UI: http://localhost:18080
```

Streaming progress JSON is saved under `/lake/observability/progress`; Spark event logs are under `/events`. The startup message that AQE is disabled applies to streaming plans; batch work in `foreachBatch` should be inspected separately in its SQL plan. See the performance document for the metrics and diagnosis workflow.

Credentials are generated in an ignored `.env` with mode `0600`, and are not printed by setup. The CDC user has replication and source read permissions; Connect resolves its password through an environment config provider. Spark runs as an unprivileged user and receives no database credentials. PostgreSQL, Kafka and Connect expose no host ports; UI ports bind to loopback. Local container traffic is plaintext inside the isolated Compose network. Production requires authenticated/TLS network connections, a secret manager, access controls, replicated services and durable shared storage.

The original upstream commits contain demo credentials; preserving upstream history means those historical literals still exist. New local credentials are different, and current application/configuration files do not embed passwords. Do not reuse any upstream example credentials.

Personal interview context and the assessment PDF remain local and ignored. Development branches separate platform setup, ingestion, processing, serving and verification; merge commits preserve the implementation path. The private delivery repository is [fabdutra/deel-data-engineering-task](https://github.com/fabdutra/deel-data-engineering-task). The designated assessment reviewers are `juan-domingos-deel` and `xi-wang-deel`; access is managed through GitHub collaborator invitations, which recipients must accept. No public upstream PR is necessary.
