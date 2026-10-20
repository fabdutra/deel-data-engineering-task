"""Narrative and editable slide layouts; all measured values come from evidence."""

from build import NAVY, TEAL, MINT, BG, WHITE, INK, MUTED, GOLD, RED, LIGHT, REPO, load


def make_slides(S, d, assets):
    slides = []

    def new(title, section="SOLUTION", sources=(), notes="", dark=False):
        s = S(title, section, sources, notes, dark)
        slides.append(s)
        return s

    s = new(
        "",
        sources=("README.md", "docs/assessment-deliverables.md"),
        dark=True,
        notes="Open with the operational problem: ACME needs trustworthy order visibility as demand grows. I built and tested the complete source-to-query path. I will explain the consistency contract first, then the measured performance constraints. The main narrative is approximately 18–20 minutes; the appendix supports technical questions. No business ROI or production SLA is claimed.",
    )
    s.text(48, 45, 840, "DEEL  /  SENIOR DATA ENGINEER ASSESSMENT", 12, "6DE0C6", True)
    s.text(
        48,
        128,
        860,
        "From operational CDC\nto trusted analytics",
        43,
        WHITE,
        True,
        leading=51,
    )
    s.text(
        50,
        259,
        825,
        "A measured Spark Structured Streaming platform\nfor ACME Delivery Services",
        23,
        "D2E4EC",
    )
    s.text(50, 361, 820, "Fabricio Dutra", 20, WHITE, True)
    s.text(50, 393, 820, "Architecture • methodology • sizing • evidence", 15, "B2CAD5")
    s.text(
        50,
        449,
        820,
        "Implemented end to end. Tested for correctness, load and recovery.",
        16,
        "6DE0C6",
        True,
    )

    s = new(
        "Four questions drive the platform",
        "BUSINESS OUTCOME",
        ("sql/open-orders.sql", "sql/pending-items.sql"),
        "Tie each output to an operational decision. Order counts must not be multiplied by joining to item rows. Pending items expose line count and total units because the wording can otherwise be ambiguous. The output is an operational current-state view, not a claimed business-impact measurement.",
    )
    s.card(
        48,
        116,
        418,
        135,
        "01  Open orders",
        "By delivery date and status\nSupport workload planning.",
    )
    s.card(
        490,
        116,
        422,
        135,
        "02  Top delivery dates",
        "The three dates with most open orders\nIdentify delivery-day concentration.",
    )
    s.card(
        48,
        271,
        418,
        135,
        "03  Pending items",
        "By product: item lines and total units\nSupport picking and storage priorities.",
    )
    s.card(
        490,
        271,
        422,
        135,
        "04  Top customers",
        "The three customers with most pending orders\nPrioritize operational follow-up.",
    )
    s.text(
        48,
        438,
        864,
        "Definition: PENDING, PROCESSING and REPROCESSING are open; COMPLETED is excluded.",
        16,
        MUTED,
    )

    s = new(
        "Make the assumptions explicit",
        "METHODOLOGY",
        ("docs/assessment-deliverables.md", "docs/architecture.md"),
        "State the interpretation before showing results. History is the initial snapshot plus persisted state from earlier batches, not SCD2 or every prior order status. The source fixture is synthetic and continuously updated. The PDF asks for low latency without a numeric SLA; 30 seconds is my experimental reference. Production arrival rates and query-concurrency targets were not supplied.",
    )
    s.card(
        48,
        116,
        272,
        190,
        "Historical contract",
        "Initial snapshot + current state accumulated from earlier CDC batches.\nNo pre-snapshot reconstruction.",
        size=16,
    )
    s.card(
        344,
        116,
        272,
        190,
        "Freshness contract",
        "Continuous microbatches.\n30s is a test reference, not a requirement supplied by Deel.",
        size=16,
    )
    s.card(
        640,
        116,
        272,
        190,
        "Deployment scope",
        "Reproducible local Docker stack.\nProduction sizing remains a measurement-driven strategy.",
        size=16,
    )
    s.rect(48, 333, 864, 116, MINT, radius=10)
    s.text(66, 348, 826, "Measured separately", 17, TEAL, True)
    s.text(
        66,
        379,
        826,
        "Correctness, bulk materialization, CDC arrival rate, publication age, recovery and query cold start.",
        19,
        INK,
    )

    s = new(
        "One complete source-to-query path",
        "ARCHITECTURE",
        ("docs/architecture.md", "docker-compose.yaml"),
        "Walk left to right: pg_cron changes PostgreSQL; Debezium snapshots and then tails WAL; Kafka carries four entity topics. Spark has two streams and two persistent checkpoints: Kafka to raw Parquet, then Bronze to foreachBatch. Silver stores ordered current state. Gold joins that historical state and writes facts/dimensions. Only a completed manifest is exposed through the separate Spark SQL CLI. Invalid events block publication and go to quarantine. Both streams share two local slots and one materializer owns the mutable tables. Secrets come from the ignored environment/provider; connector initialization is one-off.",
    )
    for x, title, body in [
        (48, "PostgreSQL + pg_cron", "Source + automatic mutations"),
        (282, "Debezium / Connect", "Snapshot → WAL CDC"),
        (516, "Kafka KRaft", "Four entity topics"),
    ]:
        s.card(x, 115, 204, 65, title, body, size=12)
    s.line(252, 146, 282, 146, arrow=True)
    s.line(486, 146, 516, 146, arrow=True)
    s.text(750, 119, 160, "Environment provider\n+ connector init", 13, MUTED)
    s.rect(48, 209, 864, 172, "E6F0F0", radius=12)
    s.text(
        64,
        218,
        820,
        "SPARK APPLICATION  /  2 shared local slots  /  single materializer",
        12,
        TEAL,
        True,
    )
    nodes = [
        (64, "1 · Bronze", "Native Parquet sink\nOriginal key/value bytes"),
        (282, "2 · Silver", "foreachBatch + Delta\nAccumulated current state"),
        (500, "Gold", "Delta facts + dimensions\nHistorical-state joins"),
        (718, "Publication", "Delta version manifest\nComplete read boundary"),
    ]
    for x, title, body in nodes:
        s.card(x, 250, 178, 99, title, body, size=13)
    for x in (242, 460, 678):
        s.line(x, 296, x + 40, 296, arrow=True)
    s.line(618, 180, 618, 194)
    s.line(618, 194, 153, 194)
    s.line(153, 194, 153, 250, arrow=True)
    s.card(
        48,
        406,
        272,
        64,
        "Persistent observability",
        "2 checkpoints • progress • event logs",
        size=12,
    )
    s.card(
        344,
        406,
        272,
        64,
        "Quarantine + Spark UI",
        "Invalid CDC • optional History Server",
        size=12,
    )
    s.card(
        640,
        406,
        272,
        64,
        "Separate Spark SQL CLI",
        "Read every table at published versions",
        size=12,
    )
    s.line(807, 349, 807, 406, arrow=True)

    s = new(
        "Two fact grains prevent double counting",
        "DIMENSIONAL MODEL",
        ("app/gold.py", "docs/architecture.md"),
        "The order fact is one row per order and is the source for order-count questions. The item fact is one row per item and carries the current parent status, date and customer for filtering. Dimensions are SCD1 with stable source identifiers, not a surrogate-key service. Dates have an unknown key of zero. Parent changes refresh historical children. Customers/products missing from dimensions do not drop operational order facts.",
    )
    s.card(48, 120, 250, 88, "dim_customer", "SCD1 • source customer key", size=14)
    s.card(356, 120, 250, 88, "dim_date", "Order / delivery date • key 0", size=14)
    s.card(662, 120, 250, 88, "dim_product", "SCD1 • source product key", size=14)
    s.card(
        124,
        273,
        315,
        112,
        "fact_order",
        "1 row / order\nStatus, dates and customer",
        fill=MINT,
    )
    s.card(
        516,
        273,
        320,
        112,
        "fact_order_item",
        "1 row / order item\nQuantity + current parent attributes",
        fill=MINT,
    )
    s.line(173, 208, 256, 273, arrow=True)
    s.line(481, 208, 344, 273, arrow=True)
    s.line(481, 208, 592, 273, arrow=True)
    s.line(787, 208, 703, 273, arrow=True)
    s.text(
        48,
        423,
        864,
        "Order metrics query fact_order directly. Item metrics count lines and sum quantities.",
        18,
        TEAL,
        True,
    )
    s.text(
        48,
        457,
        864,
        "The source field “quanity” becomes quantity; prices use explicit DECIMAL(20,4) casts.",
        14,
        MUTED,
    )

    s = new(
        "Every microbatch earns its publication",
        "PROCESSING",
        ("app/materialize.py", "app/storage.py"),
        "Bronze retains the exact Kafka key/value bytes and coordinates, including tombstones. The second stream validates before mutating current-state tables, selects the latest event by LSN/offset and conditionally merges Silver. Gold recomputes affected entities by joining the stored state. Only after all writes succeed is a single Delta manifest published. Most transformations use PySpark DataFrames and Delta merge; serving queries are Spark SQL. The pipeline does not collect business datasets to the driver.",
    )
    steps = [
        ("1  Preserve", "Kafka → native Parquet\nCheckpoint + sink metadata"),
        (
            "2  Validate",
            "Decode schema and business rules\nInvalid CDC → quarantine + fail",
        ),
        ("3  Order + apply", "Latest LSN / Kafka offset\nConditional Silver MERGE"),
        (
            "4  Enrich history",
            "Changed items ↔ stored orders\nChanged orders ↔ stored items",
        ),
        (
            "5  Refresh Gold",
            "Affected facts and SCD1 dimensions\nRetain logical delete guards",
        ),
        ("6  Publish", "Commit complete table versions\nCLI uses versionAsOf"),
    ]
    for i, (title, body) in enumerate(steps):
        s.card(48 + (i % 3) * 296, 116 + (i // 3) * 165, 272, 141, title, body, size=15)
    s.text(
        48,
        461,
        864,
        "PySpark DataFrames + Delta MERGE for processing; Spark SQL for the four serving queries.",
        15,
        TEAL,
        True,
    )

    s = new(
        "Historical joins are part of the stream",
        "HISTORY",
        ("app/gold.py", "docs/evidence/historical-join-tests.log"),
        "Use this illustrative sequence to explain the integration test pattern, not as a captured source record. First an order enters by snapshot. Later a new item arrives with no order event in that batch; its parent status/customer/date come from Silver. Later the order completes with no new item events; stored items are refreshed from the parent change. This is why a watermark-based stream-stream window join would be the wrong contract for long-lived operational orders.",
    )
    for x, number, title, event, lookup, result in [
        (
            48,
            "01",
            "Snapshot baseline",
            "Order O1 is PENDING",
            "Store the order in Silver",
            "Order exists in Gold",
        ),
        (
            344,
            "02",
            "Later item event",
            "New item I1 references O1",
            "Join I1 with stored order O1",
            "Item inherits parent state",
        ),
        (
            640,
            "03",
            "Later parent update",
            "Order O1 becomes COMPLETED",
            "Retrieve its stored item I1",
            "Pending item disappears",
        ),
    ]:
        s.text(x, 116, 272, number, 38, TEAL, True)
        s.text(x, 173, 272, title, 19, INK, True)
        s.card(
            x,
            213,
            272,
            200,
            "Event → historical lookup",
            event + "\n\n" + lookup + "\n\n" + result,
            size=15,
        )
    s.text(
        48,
        445,
        864,
        "Illustrative sequence based on tested behavior. “History” means retained state, not SCD2.",
        15,
        MUTED,
    )

    s = new(
        "Choose the simplest components that fit",
        "ENGINEERING DECISIONS",
        ("docs/architecture.md", "README.md"),
        "Explain the choices as tradeoffs. Parquet Bronze is append-only raw event preservation; Delta is reserved for mutable current state and dimensional tables. Native checkpoints/metadata must remain intact. The publication manifest provides consistent supported reads but not global source transaction buffering. CLI is an allowed assessment serving option, with a documented cold-start/concurrency cost. Avoid presenting the local plaintext network or a single Kafka broker as production security or availability.",
    )
    s.table(
        48,
        117,
        [180, 365, 319],
        ["Decision", "Reason", "Tradeoff"],
        [
            [
                "Parquet Bronze",
                "Preserve original events; native append sink",
                "Keep files, metadata and checkpoint together",
            ],
            [
                "Delta Silver / Gold",
                "Ordered upserts, delete guards, versioned reads",
                "MERGE and retained-target scans have cost",
            ],
            [
                "Publication manifest",
                "Expose a completed set of table versions",
                "Supported readers must honor the manifest",
            ],
            [
                "On-demand SQL CLI",
                "Deliver the four required queries with few services",
                "Cold start; not a high-QPS query server",
            ],
        ],
        size=15,
        rowh=62,
    )
    s.text(
        48,
        437,
        864,
        "Security: environment-based secrets, restricted CDC account, unprivileged Spark, loopback UIs.",
        15,
        TEAL,
        True,
    )
    s.text(
        48,
        465,
        864,
        "TLS, replicated services and shared durable storage are production additions.",
        14,
        MUTED,
    )

    s = new(
        "Size for working sets and recovery",
        "SPARK SIZING",
        ("docs/performance.md", "docker-compose.yaml"),
        "Be precise: local[2] is two task slots sharing one driver JVM, not two executors. Heap is 2 GiB and the container cap is 3 GiB. Four shuffle partitions are a measured starting configuration, not an optimum from an exhaustive A/B study. Both streams compete for these slots. Variables include actual event rate/bytes, historical target size, mutation mix, parent fanout, file layout and desired recovery/freshness. Production must separately size executors and driver on shared durable storage.",
    )
    s.stat(
        48, 119, 272, "2 slots", "Shared by both streams", "local[2] • two-CPU quota"
    )
    s.stat(344, 119, 272, "2 GiB", "JVM heap", "3 GiB total container cap")
    s.stat(
        640, 119, 272, "4", "Shuffle partitions", "Starting choice, not proven optimum"
    )
    s.table(
        48,
        252,
        [220, 310, 334],
        ["Budget / control", "Local configuration", "Sizing implication"],
        [
            [
                "Kafka / Connect / PostgreSQL",
                "1 GiB / 1 GiB / 512 MiB",
                "Fit source, CDC and Spark in ~8 GB VM",
            ],
            [
                "Streaming intake",
                "20k offsets / 10s; 32 files / batch",
                "Bounds intake, not fanout or task memory",
            ],
            [
                "Backlog recovery",
                "Required rate ≥ λ + B / R",
                "Arrival λ; backlog B; recovery window R",
            ],
        ],
        size=15,
        rowh=57,
    )
    s.text(
        48,
        469,
        864,
        "Scale from measured CPU, GC/spill, task balance, target scans and freshness—not row count alone.",
        14,
        TEAL,
        True,
    )

    s = new(
        "Triangulate correctness and performance",
        "VALIDATION METHOD",
        ("tests/test_pipeline.py", "docs/evidence/README.md"),
        "Separate evidence types. Real Spark/Delta integration tests cover ordering, duplicate events, deletes, late parents, invalid events and crash/replay. PostgreSQL is an independent oracle for all four business metrics. Bulk materialization is measured independently from Kafka/Bronze and full CDC load. Physical checks compare Kafka coordinates and published input counts. There is one execution per stress scenario and approximately two-minute arrival steps, not a statistically certified soak. Captured result files are actual outputs, not fabricated screenshots.",
    )
    s.card(
        48,
        117,
        418,
        142,
        "Semantic tests",
        "Snapshot + CDC • ordering • deletes\nHistorical joins • quarantine • crash / replay",
        size=17,
    )
    s.card(
        490,
        117,
        422,
        142,
        "Independent source oracle",
        "Compare all four business queries against PostgreSQL before / after changes.",
        size=17,
    )
    s.card(
        48,
        282,
        418,
        142,
        "Separate workloads",
        "Bulk fixture • intake bursts • stepped CDC\nHot parent • query concurrency • outage",
        size=17,
    )
    s.card(
        490,
        282,
        422,
        142,
        "Evidence and accounting",
        "Native task logs + stream progress\nUnique Kafka coordinates + publication counts",
        size=17,
    )
    s.text(
        48,
        454,
        864,
        "Synthetic data; short steps; actual rates reported; no production SLA inferred.",
        17,
        TEAL,
        True,
    )

    uniform = sorted(
        [r for r in d["materialization"] if not r["hot"]], key=lambda r: r["events"]
    )
    big = uniform[-1]
    s = new(
        "Gold dominates as retained state grows",
        "PERFORMANCE / BULK",
        (
            "docs/evidence/stress/materialization.json",
            "docs/evidence/stress/scale-1m-spark-metrics.json",
        ),
        "The initial materialization grows from 40.6 to 139.2 to 370.5 seconds across roughly 0.6, 3 and 6 million events. Timings exclude Spark initialization, separate Bronze writing and later verification. Gold takes 232.5 seconds, approximately 63% of the largest initial load. Task disk spill is 846 MB across the complete largest application, not just this plotted initial phase. The cgroup probe reaches approximately its cap without OOM. Do not reuse bulk events/s as a streaming capacity promise.",
    )
    s.image(assets / "materialization.png", 48, 118, 595, 314)
    s.stat(
        665,
        119,
        247,
        f"{big['initial_seconds']:.1f}s",
        "6,000,200 input events",
        "Initial materialization",
    )
    s.stat(665, 247, 247, "63%", "Time in Gold refresh", "232.5s of 370.5s")
    s.text(
        665,
        382,
        247,
        "846 MB disk spill\nMemory pressure near cap\n0 failed task attempts",
        17,
        RED,
        True,
    )
    s.text(
        48,
        461,
        864,
        "Doubling ~3m → ~6m events increased initial materialization time by 2.66×.",
        16,
        TEAL,
        True,
    )

    s = new(
        "Freshness degrades without container failures",
        "PERFORMANCE / STREAMING",
        (
            "docs/evidence/stress/streaming-levels.json",
            "docs/evidence/stress/streaming-batches.json",
        ),
        "The x-axis uses measured arrival rates: 235, 954, 2356 and 2587 events/s. The nominal 6000 events/s target was not achieved. Each age p95 is computed within a batch from source timestamps, excluding snapshot. The plotted median/maximum of batch p95 values is not a global event p95. Above about 2000 events/s, the original intake cap contributes growing lag. Even the smallest stage has a batch p95 above the chosen 30-second reference. Backlogs drain after load stops, but that is different from preserving freshness while arrivals continue.",
    )
    s.image(assets / "freshness.png", 48, 117, 595, 314)
    s.card(
        665,
        119,
        247,
        143,
        "Reference breached",
        "235 events/s: largest batch p95 = 32.5s\n2,587 events/s: 112.5s",
        size=16,
        accent=GOLD,
    )
    s.card(
        665,
        283,
        247,
        143,
        "Catch-up observed",
        "All four steps drained.\nHighest two steps: ~66s and ~70s after load stopped.",
        size=16,
    )
    s.text(
        48,
        452,
        864,
        "Per-batch age summaries—not global event p95. Steps ~2 minutes; nominal 6k/s was not reached.",
        14,
        MUTED,
    )

    totals = d["integrity"]["aggregate_reconciliation"]["gold"]
    s = new(
        "Correctness survives a temporary broker outage",
        "INTEGRITY / RECOVERY",
        (
            "docs/evidence/stress/final-integrity.json",
            "docs/evidence/stress/broker-outage.jsonl",
        ),
        "Three independent comparisons of all four business metrics passed: after load, after 12375 parent updates and 6378 item deletes, and after broker recovery. Final Kafka offsets, raw Bronze rows/unique coordinates and successfully published raw input counts all agree at 774001. These are event coordinates, not unique business entities. The broker was stopped, 6000 source events committed, and the supervisor restarted Kafka. 53.5 seconds includes stop/wait/start/process/poll; Spark did not need a restart. This proves temporary outage recovery with retained volumes, not HA or disk-loss recovery.",
    )
    s.stat(
        48,
        118,
        272,
        "774,001",
        "Kafka events = Bronze rows",
        "Also unique Kafka coordinates",
    )
    s.stat(
        344,
        118,
        272,
        "3 / 3",
        "Metric reconciliations passed",
        "All four business questions",
    )
    s.stat(
        640,
        118,
        272,
        "53.5s",
        "Observed recovery interval",
        "Stop → restart → catch-up → poll",
    )
    s.card(
        48,
        263,
        418,
        171,
        "Mutations exercised",
        "12,375 order updates\n6,378 item deletes + tombstones\n6,000 source events while Kafka was down",
        size=17,
    )
    s.card(
        490,
        263,
        422,
        171,
        "Final source / Gold agreement",
        f"{totals['ordercounts']['total']:,} orders\n{totals['itemcounts']['total']:,} active items\n{totals['itemcounts']['units']:,} units",
        size=17,
    )
    s.text(
        48,
        459,
        864,
        "Retained volumes / offsets / checkpoints. This is recovery evidence, not single-broker HA.",
        15,
        TEAL,
        True,
    )

    s = new(
        "Scale the measured constraint first",
        "NEXT EXPERIMENTS",
        ("docs/performance-stress.md", "docs/performance.md"),
        "Separate implemented behavior from proposed production changes. First measure retained-target scans and Gold merge/layout costs, including the expensive date-dimension work. Then compare memory, task sizes and partitions under a defined freshness objective. Decouple ingestion/materialization resources when competition is the constraint. Production needs object/shared storage and replicated Kafka with security/monitoring. A single parent updating a million children took 14.1 seconds, illustrating why offsets alone do not size work. No claimed fixed executor count follows from this local run.",
    )
    s.table(
        48,
        117,
        [220, 330, 314],
        [
            "Observed constraint",
            "Candidate action—not yet validated",
            "Acceptance measurement",
        ],
        [
            [
                "Gold / target work dominates",
                "Benchmark layout, compaction and affected-row access",
                "Files scanned + batch time + correctness",
            ],
            [
                "Heap / CPU pressure",
                "Compare task input, memory, cores and shuffle count",
                "Spill, GC, queued tasks and freshness",
            ],
            [
                "Backlog and shared slots",
                "Tune intake with downstream capacity; isolate resources",
                "Stable lag and catch-up within target R",
            ],
            [
                "Production durability / serving",
                "Shared lake, replicated Kafka; persistent serving",
                "Outage tests and latency under concurrency",
            ],
        ],
        size=14.5,
        rowh=66,
    )
    s.text(
        48,
        456,
        864,
        "One parent update refreshed 1m children in 14.1s: event count alone does not size the workload.",
        15,
        TEAL,
        True,
    )

    s = new(
        "Inspect, reproduce and discuss",
        "HANDOVER / DEMO",
        ("README.md", "docs/evidence/README.md"),
        "Close the core narrative with a clear handover. The repository contains application code, SQL, Docker, tests, measured evidence, documentation and the source for this presentation. The standard source creates and mutates data automatically. During a demo, wait for the published-batch log before querying and point out the manifest timestamp. Distinguish data freshness from a fresh JVM cold start. The optional technical appendix follows. Do not run benchmarks alongside the demo on the small VM.",
    )
    s.rect(48, 119, 864, 176, NAVY, radius=10)
    s.text(68, 136, 824, "python3 scripts/setup.py", 18, WHITE)
    s.text(68, 179, 824, "docker compose up -d --build", 18, WHITE)
    s.text(
        68,
        222,
        824,
        "docker compose run --rm --no-deps query metrics all",
        18,
        "6DE0C6",
    )
    s.text(
        48,
        324,
        864,
        "Wait for the first published batch, then inspect the four metrics and publication timestamp.",
        18,
        INK,
    )
    s.text(
        48,
        377,
        864,
        "Working system + measurable limits + a defensible path to production sizing.",
        22,
        TEAL,
        True,
    )
    s.text(
        48,
        432,
        864,
        "Code and all retained analysis: fabdutra/deel-data-engineering-task",
        17,
        INK,
        link=REPO,
    )
    s.text(
        48,
        463,
        864,
        "Appendix: CDC semantics, failure handling, intake / serving, native metrics and evidence.",
        14,
        MUTED,
    )

    s = new(
        "Upserts and deletes preserve ordering",
        "APPENDIX A / CDC",
        ("app/cdc.py", "app/storage.py"),
        "Silver orders every entity by primary key plus PostgreSQL LSN and Kafka offset. The per-key partition-ordering assumption must be preserved during topic changes. Deletes use logical markers that retain ordering so stale updates cannot resurrect data. Kafka null-value tombstones are preserved in Bronze and do not delete a second time. Customer/product dimensions are SCD1; facts exclude logical deletions in supported queries. Date members are merged by date key and retained. Missing parents are retained in Silver and repaired when the parent arrives.",
    )
    s.table(
        48,
        116,
        [170, 210, 270, 214],
        ["Layer / table", "Identity", "Mutation behavior", "Delete behavior"],
        [
            [
                "Silver: 4 entities",
                "Source primary key",
                "Latest LSN / offset; conditional MERGE",
                "Retain _deleted ordering guard",
            ],
            [
                "Gold: fact_order",
                "order_id",
                "Refresh changed orders",
                "Logical deletion; CLI filters",
            ],
            [
                "Gold: fact_order_item",
                "order_item_id",
                "Changed items + stored children of changed parents",
                "Parent/item state drives visibility",
            ],
            [
                "Gold: customer / product",
                "Stable source key",
                "SCD1 MERGE",
                "Retain delete marker",
            ],
            [
                "Gold: dim_date",
                "date_key",
                "Merge affected date members; unknown = 0",
                "Members retained",
            ],
        ],
        size=14,
        rowh=52,
    )
    s.text(
        48,
        457,
        864,
        "Topic repartitioning or source-identity changes require a controlled ordering migration.",
        14,
        TEAL,
        True,
    )

    s = new(
        "Publish a complete read boundary",
        "APPENDIX B / FAILURES",
        ("app/materialize.py", "app/metrics.py"),
        "Each Delta transaction is per table. The manifest is the cross-table read contract: capture it once and use versionAsOf for each Gold table. If a batch fails after Silver or some Gold writes, the old manifest remains readable and replay converges from authoritative Silver. After publication, readers can see the new complete versions. Quarantine blocks the batch until parser/validation is corrected; a later source correction does not skip an invalid retained event. Kafka failOnDataLoss is true and source transactions may be split across topic batches.",
    )
    s.table(
        48,
        118,
        [250, 300, 314],
        ["Failure point", "What supported readers see", "Recovery action"],
        [
            [
                "Before / during Silver merge",
                "Previous complete publication",
                "Retain checkpoint; correct failure; replay",
            ],
            [
                "After Silver / partial Gold",
                "Previous published Gold versions",
                "Recompute affected Gold rows; publish last",
            ],
            [
                "After manifest commit",
                "New complete version set",
                "Checkpoint replay remains convergent",
            ],
            [
                "Invalid CDC contract",
                "Previous publication; batch withheld",
                "Inspect quarantine; fix parser / contract",
            ],
        ],
        size=15,
        rowh=65,
    )
    s.text(
        48,
        441,
        864,
        "No global atomic buffering of source transactions across topics. One materializer owns the tables.",
        15,
        TEAL,
        True,
    )
    s.text(
        48,
        468,
        864,
        "Missing Kafka offsets fail visibly. Checkpoints and raw Parquet metadata must stay intact.",
        14,
        MUTED,
    )

    s = new(
        "Intake and serving have different limits",
        "APPENDIX C / OTHER LOAD TESTS",
        (
            "docs/evidence/stress/bronze-load-levels.jsonl",
            "docs/evidence/stress/query-concurrency.jsonl",
        ),
        "Ingestion uses a separate two-partition synthetic topic and no Gold work. With the same checkpoint/resources, a 250000-record, 500-byte burst drains in 127.8 seconds at 20000 offsets and 20.9 seconds at 200000 offsets. Payloads are synthetic and do not guarantee realistic compression, so this is not broker disk saturation. Query concurrency tests use fresh separate JVMs, a quiet source and a live pipeline; each executes four metrics plus two checks. They show cold-start/contention cost, not peak interactive throughput under maximum writes.",
    )
    s.text(48, 116, 418, "Kafka → Bronze only", 20, INK, True)
    s.table(
        48,
        157,
        [140, 140, 138],
        ["Payload", "20k cap", "200k cap"],
        [["500 B / 250k", "127.8s", "20.9s"], ["5k B / 100k", "57.6s", "20.4s"]],
        size=14,
        rowh=62,
    )
    s.text(490, 116, 422, "Fresh query containers", 20, INK, True)
    s.table(
        490,
        157,
        [212, 210],
        ["Concurrent processes", "Group time"],
        [["1", "24.6s"], ["2", "37.9s"], ["4", "65.2s"]],
        size=15,
        rowh=47,
    )
    s.card(
        48,
        354,
        418,
        110,
        "What this demonstrates",
        "The original admission cap limits these bursts; larger intake is not a Gold fix.",
        size=15,
    )
    s.card(
        490,
        354,
        422,
        110,
        "What remains open",
        "Query concurrency at peak write load and an always-on serving comparison.",
        size=15,
    )

    metrics = [
        load("docs/evidence/stress/" + name + "-spark-metrics.json")
        for name in ("scale-100k", "scale-500k", "scale-1m")
    ]
    s = new(
        "Native metrics support the sizing discussion",
        "APPENDIX D / SPARK METRICS",
        (
            "docs/evidence/stress/scale-1m-spark-metrics.json",
            "docs/evidence/stress/online-spark-metrics.json",
        ),
        "Read these as summed task measurements across each complete application. CPU/run time can exceed wall time, and task GC accounting can overlap. Memory spill is an estimated deserialized volume, not extra physical RAM. The table mixes stages, so its global task p95 is not proof of skew. The streaming run had 11487 task attempts with no failures and no task spill; that does not contradict the large offline fixture spilling because its admitted working set is very different. The cgroup sample records 673 memory-limit events and no OOM.",
    )
    rows = []
    for label, m in zip(("100k orders", "500k orders", "1m orders"), metrics):
        t = m["totals"]
        rows.append(
            [
                label,
                str(t["task_attempts"]) + " / " + str(t["failed_task_attempts"]),
                f"{t['executor_cpu_ns']/1e9:.1f}s",
                f"{t['jvm_gc_ms']/1000:.1f}s",
                f"{t['disk_bytes_spilled']/1e6:.1f} MB",
                f"{m['task_run_ms_p95']/1000:.3f}s",
            ]
        )
    s.table(
        48,
        118,
        [165, 145, 138, 138, 140, 138],
        ["Fixture", "Tasks / fail", "CPU sum", "GC sum", "Disk spill", "Task p95"],
        rows,
        size=14,
        rowh=59,
    )
    s.card(
        48,
        366,
        418,
        105,
        "Largest offline fixture",
        "~3 GiB cgroup peak; CPU throttling\nNo OOM; measurable memory pressure.",
        size=15,
    )
    s.card(
        490,
        366,
        422,
        105,
        "Streaming task extract",
        "11,487 attempts / 0 failures\n337.4s CPU; 22.7s task GC; no spill.",
        size=15,
    )

    q = d["query"]
    s = new(
        "Captured business output is inspectable",
        "APPENDIX E / ACTUAL OUTPUT",
        (
            "docs/evidence/stress/query-output.json",
            "docs/evidence/stress/reconciliation.jsonl",
        ),
        "These values are selected from the retained actual query JSON, not a screenshot or a simulated live UI. The publication is batch 39 with pinned Gold versions. Show one row from each of the four metrics; the complete result remains linked. This synthetic load deliberately concentrates new orders on customer 1 and product 1, so the top-customer/product distribution is not representative production demand. All four metrics were compared independently with PostgreSQL.",
    )
    s.text(
        48,
        117,
        864,
        f"Publication batch {q['publication']['batch_id']}  •  {q['publication']['published_at']} UTC",
        16,
        TEAL,
        True,
    )
    o = q["metrics"]["open-orders"]["rows"][0]
    date = q["metrics"]["top-delivery-dates"]["rows"][0]
    item = q["metrics"]["pending-items"]["rows"][0]
    customer = q["metrics"]["top-customers"]["rows"][0]
    s.table(
        48,
        166,
        [210, 440, 214],
        ["Metric", "Selected actual group", "Result"],
        [
            [
                "Open orders",
                f"{o['delivery_date']} / {o['status']}",
                f"{o['open_orders']:,} orders",
            ],
            [
                "Top delivery date",
                date["delivery_date"],
                f"{date['open_orders']:,} orders",
            ],
            [
                "Pending items",
                f"Product {item['product_id']}",
                f"{item['pending_item_lines']:,} lines\n{item['pending_units']:,} units",
            ],
            [
                "Top customer",
                f"Customer {customer['customer_id']} — {customer['customer_name']}",
                f"{customer['pending_orders']:,} orders",
            ],
        ],
        size=14.5,
        rowh=59,
    )
    s.text(
        48,
        463,
        864,
        "Selected rows only. Full outputs and independent source comparisons are retained as JSON.",
        14,
        MUTED,
    )

    s = new(
        "Limits are part of the analytical result",
        "APPENDIX F / INTERPRETATION",
        ("docs/performance-stress.md", "docs/assessment-deliverables.md"),
        "Be ready to explain the gaps without overclaiming. This is a complete assessment implementation, not production certification. Historical semantics, atomicity and single-writer constraints are deliberate. Arrival-rate steps are short and pauses affect actual rates; the nominal 6000 target was not attained. Batch age quantiles are not event-global p95. Snapshot replay emitted the initial 185 records twice; raw coordinates and business reconciliation still matched. A minimum heap, optimum partitions or independently saturated source/broker capacity were not established.",
    )
    s.card(
        48,
        117,
        418,
        150,
        "Contract limits",
        "Current state, not SCD2\nPer-table transactions + manifest reads\nSingle materializer; local durable volumes",
        size=16,
    )
    s.card(
        490,
        117,
        422,
        150,
        "Measurement limits",
        "One run / scenario; ~2-minute steps\nActual peak input ~2.59k/s, not 6k/s\nBatch p95 summaries ≠ global event p95",
        size=16,
    )
    s.card(
        48,
        290,
        418,
        172,
        "Replay observed",
        "Initial 185-row snapshot emitted twice.\nBronze retained both coordinate sets.\nFinal checks used actual offsets; business-state reconciliation passed.",
        size=16,
    )
    s.card(
        490,
        290,
        422,
        172,
        "Production questions still open",
        "Long-duration SLA and workload mix\nDistributed executor scaling curve\nBroker / CDC / source saturation\nRemote storage and peak query writes",
        size=16,
    )

    s = new(
        "Every claim has a path back to the work",
        "APPENDIX G / DELIVERABLE INDEX",
        ("docs/assessment-deliverables.md", "docs/evidence/README.md"),
        "Finish technical discussion by pointing to the reviewer index in README. All underlying implementation and retained evidence are in the private repository. This presentation has a reproducible source generator, pinned dependencies, exported outline/English speaker notes and a SHA-256 manifest of inputs/outputs. The private Portuguese study guide is intentionally not part of the submission. Rebuilding the deck uses existing evidence and does not start Docker or rerun the historical performance campaign.",
    )
    entries = [
        ("Application + SQL", "app/ and sql/", "README.md"),
        (
            "Docker + run instructions",
            "docker-compose.yaml and README",
            "docker-compose.yaml",
        ),
        (
            "Architecture + historical joins",
            "docs/architecture.md",
            "docs/architecture.md",
        ),
        (
            "Sizing + measured limits",
            "docs/performance.md and performance-stress.md",
            "docs/performance-stress.md",
        ),
        (
            "Evidence + experiment code",
            "docs/evidence/ and scripts/stress/",
            "scripts/stress/README.md",
        ),
        (
            "Presentation source + provenance",
            "scripts/presentation/ and docs/presentation/manifest.json",
            "scripts/presentation/build.py",
        ),
    ]
    for i, (label, path, link) in enumerate(entries):
        y = 116 + i * 52
        s.text(48, y, 280, label, 16, INK, True)
        s.text(340, y, 572, path, 15, TEAL, link=REPO + "/blob/main/" + link)
    s.text(
        48,
        454,
        864,
        "fabdutra/deel-data-engineering-task  •  private assessment delivery",
        18,
        TEAL,
        True,
        link=REPO,
    )
    return slides
