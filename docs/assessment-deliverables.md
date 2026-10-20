# Assessment deliverables and evidence

This is the reviewer's entry point for the implementation, its evidence and its explicit limits. The architecture, sizing and metrics documents are versioned in this repository; the candidate's personal notes under `context/` are excluded.

The recruiter's additional presentation requirement is covered by the [PDF](presentation/acme-delivery-assessment.pdf), [editable PowerPoint](presentation/acme-delivery-assessment.pptx) and [submission package guide](submission.md). The underlying [experiment code](../scripts/stress/README.md), [presentation/figure source](../scripts/presentation/) and recorded evidence accompany the application implementation.

## Requirements mapped to the implementation

| Assessment requirement | Implementation | Evidence / reference |
|---|---|---|
| Process provided Debezium topics with Spark Structured Streaming | A Kafka source consumes customers, products, orders and order items; a second streaming query materializes Bronze microbatches | `app/bronze.py`, `app/pipeline.py`; [live reconciliation](evidence/live-reconciliation.json) |
| Join streaming changes with historical data | CDC updates the accumulated Silver state; affected items join stored orders, and changed orders retrieve stored items, including records with no event in the current batch | [Historical-data join contract](architecture.md#joining-cdc-with-historical-data); `refresh_gold()` and `item_fact()` in `app/gold.py`; explicit integration scenario in `tests/test_pipeline.py` and [recorded execution](evidence/historical-join-tests.log) |
| Query historical information together with current orders' state | The initial snapshot supplies preexisting orders; later CDC updates/inserts are integrated into the same retained order/item facts | [History semantics below](#what-historical-data-means-here); snapshot, new-order and cross-batch assertions in the integration test |
| Consistent dimensional output | Delta order and order-item facts, customer/product/date dimensions; a publication manifest pins a complete set of table versions for CLI readers | [Dimensional grains and consistency](architecture.md); `app/gold.py`, `app/metrics.py` |
| Open orders by delivery date and status | Query the order fact directly, avoiding multiplication by item count | `sql/open-orders.sql` |
| Top three delivery dates with most open orders | Aggregate open orders by delivery date with a deterministic tiebreaker | `sql/top-delivery-dates.sql` |
| Pending items by product | Return both pending item-line count and sum of pending units, filtered using the parent's current order status | `sql/pending-items.sql` |
| Top three customers with most pending orders | Count the order fact and join the customer dimension | `sql/top-customers.sql` |
| Data accessible with near-real-time updates | Continuous microbatch processing; the supported CLI reads the latest completed publication on each invocation | [CLI commands](../README.md#run); [actual CLI output](evidence/cli-metrics.txt); freshness limits below |
| Architecture diagram with all components | Source/CDC, Kafka, Spark queries, storage, publication, CLI, quarantine, progress and event logs | [Architecture diagram](architecture.md#architecture-and-data-contracts); container definitions in `docker-compose.yaml` |
| Spark cluster sizing strategy and decision variables | Local CPU/heap/container budgets, rate limits, workload variables, production capacity formulas and bottleneck-specific scaling actions | [Sizing and performance](performance.md) |
| Key Spark metrics, for bonus evidence | Task CPU/run time, GC, shuffle, spill, failures and streaming batch durations, with collection scripts | [Initial metrics](performance.md#key-spark-metrics-used); [stress metrics and sizing implications](performance-stress.md) |
| Evidence that the application runs | Real Spark/Delta tests, CLI output, source/Gold reconciliations, larger materialization/load tests and broker recovery | [Evidence index](evidence/README.md); [captured stress query result](evidence/stress/query-output.json); [final event/count integrity](evidence/stress/final-integrity.json) |
| Dockerized application and detailed README | Compose stack, image builds, credential setup, startup/query/validation/recovery instructions | [README](../README.md), `docker/`, `scripts/setup.py` |
| Preserve development history with clear commits | Upstream history retained; implementation separated into branches and incremental commits | Repository commit graph; private independent copy rather than a public native fork |
| Share a private delivery with the named reviewers | Private repository under `fabdutra`; access uses GitHub collaborator invitations | Designated reviewers: `juan-domingos-deel` and `xi-wang-deel`; recipients must accept their invitations |

## What historical data means here

The implemented contract is **preexisting source records plus the current state accumulated from earlier CDC batches**. No separate historical dataset is supplied by the starter; the initial Debezium snapshot establishes the baseline. Silver retains that baseline and subsequent updates, including orders that have no new event for a long time.

Two examples demonstrate an actual join with that accumulated data:

1. An order is loaded by the initial snapshot. Several batches later, a new item arrives with only the order's ID. `item_fact()` joins the stored order to obtain its status, customer and delivery date. Without the historical order row, those attributes cannot be derived from the new event alone.
2. A CDC event marks an existing order completed. `refresh_gold()` joins the changed order IDs to items stored in Silver from previous batches. Those historical item rows are refreshed so their pending metrics change even though the source emits no new event for each item.

The joins run as batch DataFrame operations within `foreachBatch`; they are part of the streaming application. The Silver inputs are reopened for each batch after applying its changes, so enrichment uses the accumulated current state rather than a dimension snapshot loaded once at application startup.

This is not SCD2 or a temporal/as-of join. It does not expose a dimensional row for every past order status, nor reconstruct changes before the first snapshot. Bronze retains the received CDC events for replay/audit. The business questions are answered from current order state across old and new orders. If “historical information” is intended to require analytical queries of every past state, that would be an additional temporal-model requirement beyond this documented contract.

## Consistency, latency and scale boundaries

- Conditional Silver MERGE and retained delete markers prevent older events from reversing newer state. The integration scenario tests duplicate delivery, crash/replay and deletion guards.
- The CLI reads the versions in one committed Gold publication. Source transactions split across topics/batches are eventually reconciled; globally atomic source-transaction buffering is not implemented.
- A ten-second trigger is a scheduling choice, not a ten-second end-to-end SLA. The live restart-followed-by-update test observed a 46.561-second upper bound including restart, CLI cold starts and polling. It does not establish steady-state p95 freshness.
- The initial benchmark contains 100,000 orders/500,000 items. The later [stress campaign](performance-stress.md) reaches one million orders/five million items, plus short CDC load steps, parent fanout, broker recovery and query concurrency. At the largest materialization size there is memory pressure/spill; at arrival rates above approximately 2,000 actual events/s backlog grows. Even the lowest CDC step does not certify a strict 30-second freshness SLA. Longer representative loads and distributed measurements remain necessary for production sizing.
- The local application uses `local[2]`, not a multi-node Spark cluster. Distributed deployment requires shared durable lake/checkpoint storage and appropriate Kafka, security and operational configuration as documented in the architecture and sizing strategy.
- The PDF gives screenshots as possible evidence. This delivery uses recorded logs, CLI output and machine-readable measurements; Spark UI and optional History Server are available for further inspection.
