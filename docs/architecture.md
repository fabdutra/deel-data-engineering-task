# Architecture and data contracts

```mermaid
flowchart LR
    P[(PostgreSQL\npg_cron synthetic source)] --> D[Debezium / Kafka Connect\nconsistent initial snapshot + CDC]
    D --> K[(Kafka KRaft\n4 entity topics)]
    K --> B[Spark Structured Streaming\nKafka → native Parquet sink]
    B --> R[(Bronze\nimmutable Kafka envelopes)]
    R --> M[Spark Structured Streaming\nforeachBatch]
    M --> S[(Silver Delta\ncurrent entities + deletion guards)]
    S --> G[(Gold Delta\nfacts + dimensions)]
    G --> V[(Publication Delta\natomic version manifest)]
    V --> C[Spark SQL CLI\n4 business metrics]
    G --> C
    M --> Q[(Quarantine Delta\ninvalid CDC blocks publication)]
    B --> O[Progress JSON / Spark event logs]
    M --> O
```

There are two independent streaming checkpoints. Kafka ingestion can continue while materialization catches up. In the small local deployment both queries share one Spark application; an unrecoverable query failure stops that application and Compose restarts it. Kafka and Bronze absorb the backlog. One materializer owns Silver/Gold/publication; multiple concurrent writers are not supported by this application.

## Storage choices

Bronze is append-only Snappy Parquet, with no interpretation of the business payload. Its fixed schema holds key/value as bytes and Kafka coordinates. This avoids schema coupling to the source and does not require table-level update semantics at ingestion. Native streaming sink metadata and checkpoints must be preserved along with its files. Reading arbitrary file globs or moving files outside that protocol weakens the commit guarantees.

Silver and Gold require mutations, so they use Delta rather than raw Parquet. Decimal values are decoded as strings at Debezium and cast explicitly downstream; date values use Debezium's epoch-day representation. All Spark sessions use UTC. Schema additions remain in Bronze; changes that break the selected fields/contract block publication and require a deliberate parser update.

The demo does not partition small tables. Partitioning by order/customer IDs would create excessive directories; partitioning mutable orders by delivery date can force expensive moves. Large deployments need measured file layout/compaction, not blanket high-cardinality partitioning. See the performance strategy.

## Dimensional grains

```mermaid
erDiagram
    dim_customer ||--o{ fact_order : customer_key
    dim_date ||--o{ fact_order : order_date_key
    dim_date ||--o{ fact_order : delivery_date_key
    dim_customer ||--o{ fact_order_item : customer_key
    dim_product ||--o{ fact_order_item : product_key
    dim_date ||--o{ fact_order_item : delivery_date_key
    fact_order ||--o{ fact_order_item : order_id
```

The fact-to-fact relationship documents lineage; the order-count queries intentionally query only the order fact. Order items carry the current parent's status/date/customer for direct operational filtering. An order change recomputes its affected items from Silver. An item change recomputes that item; unrelated facts are not rewritten. Dimensions use SCD1 and stable source IDs rather than an additional surrogate-key allocation service.

## Consistency boundary

Each Silver key retains `_lsn`, `_offset`, `_partition` and `_deleted`. Within a batch, window ordering selects the latest version; across batches, MERGE updates only on a greater LSN, or a greater offset at equal LSN. This relies on the same Kafka key retaining its partition ordering; topic repartitioning/identity changes need a controlled migration. Deletion guards are not automatically purged.

After all fact/dimension writes succeed, a single-row `gold/publication` transaction records their Delta versions, source batch ID, input count, timestamp and materialization duration. A reader captures this row once and uses `versionAsOf` for every table. A crash before publication exposes none of the incomplete table changes through the supported CLI. A retry rereads authoritative Silver state and regenerates affected Gold rows before publishing. Source CDC transactions split across topics/batches are eventually reconciled; no globally atomic source-transaction view is claimed.

The source's initial snapshot supplies preexisting records; Bronze preserves later events. Delta time travel and Bronze retention support debugging/replay, but the dimensional contract remains current state. A long-lived historical state reporting requirement would need an additional versioned fact/SCD2 design.

## Production boundary

Named local volumes deliberately make the demo reproducible without adding an object-store emulator or a query server. A distributed Spark deployment must replace these with durable storage accessible at the same URI from every executor, using Delta's supported storage/log-store configuration. Merely changing `--master` does not turn local disk into distributed storage.

Production adds replicated Kafka, checkpoint/storage durability, authenticated/TLS connections, secrets management, source WAL/slot monitoring, data retention, a single-writer deployment policy and alerts. An API or always-on SQL engine can replace the CLI when query concurrency or interactive response time justifies another service; it must honor the publication version contract.
