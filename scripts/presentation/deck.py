"""Ten concise slides: architecture, deliverables and measured performance."""

from build import NAVY, TEAL, MINT, WHITE, INK, MUTED, GOLD, RED, REPO


def make_slides(S, d, assets):
    slides = []

    def new(title, section, sources, notes, dark=False):
        slide = S(title, section, sources, notes, dark)
        slides.append(slide)
        return slide

    s = new(
        "",
        "",
        ("README.md", "docs/assessment-deliverables.md"),
        "Present ten slides in about 10–12 minutes, with no appendix. Explain the architecture, deliverables and measured performance. ACME needs current order information for operational planning. This is a working assessment implementation, not a certified production cluster.",
        True,
    )
    s.text(48, 48, 864, "DEEL / SENIOR DATA ENGINEER ASSESSMENT", 12, "6DE0C6", True)
    s.text(
        48, 147, 864, "ACME Delivery\nStreaming analytics", 44, WHITE, True, leading=52
    )
    s.text(
        48,
        285,
        864,
        "Architecture, deliverables and measured performance",
        23,
        "D2E4EC",
    )
    s.text(48, 374, 864, "Fabricio Dutra", 21, WHITE, True)
    s.text(
        48,
        433,
        864,
        "From source changes to queryable order metrics.",
        20,
        "6DE0C6",
        True,
    )

    s = new(
        "What was delivered",
        "DELIVERABLES",
        ("docs/assessment-deliverables.md", "sql/pending-items.sql"),
        "The CLI answers all four business questions. Pending items expose both line count and total units. The repository also contains the architecture, sizing strategy, application, SQL, tests, experiment code and recorded evidence. No business ROI is claimed from synthetic data.",
    )
    s.card(
        48,
        119,
        418,
        145,
        "Four business queries",
        "Open orders by date and status\nTop delivery dates\nPending items by product\nTop customers",
        size=16,
    )
    s.card(
        490,
        119,
        422,
        145,
        "A complete data pipeline",
        "PostgreSQL → Debezium → Kafka → Spark\nRaw events and Delta query tables\nQueries available through a CLI",
        size=16,
    )
    s.card(
        48,
        287,
        418,
        145,
        "Architecture and sizing",
        "Component diagram and data model\nSpark resource budgets\nAssumptions and scaling decisions",
        size=16,
    )
    s.card(
        490,
        287,
        422,
        145,
        "Code and execution evidence",
        "Docker setup, application and SQL\nTests and source/Gold comparisons\nPerformance results and experiment code",
        size=16,
    )
    s.text(
        48,
        458,
        864,
        "The README links each requirement to its implementation and evidence.",
        17,
        TEAL,
        True,
    )

    s = new(
        "How the components connect",
        "ARCHITECTURE",
        ("docs/architecture.md", "docker-compose.yaml"),
        "Debezium loads the initial snapshot and captures PostgreSQL WAL changes. Kafka has four entity topics. Spark has two streams and checkpoints: Kafka to Parquet Bronze, then Bronze to foreachBatch for Silver/Gold. Both streams share two local slots. One materializer owns the tables. A completed version manifest is the supported read boundary. Invalid events go to quarantine and block publication. Secrets use the environment/provider. Local plaintext traffic and a single broker are not production TLS/HA.",
    )
    for x, title, body in [
        (48, "PostgreSQL", "Source + automatic updates"),
        (282, "Debezium / Connect", "Initial snapshot + changes"),
        (516, "Kafka", "Four entity topics"),
    ]:
        s.card(x, 115, 204, 65, title, body, size=12)
    s.line(252, 146, 282, 146, arrow=True)
    s.line(486, 146, 516, 146, arrow=True)
    s.text(
        750, 119, 160, "Secrets from environment\nOne-off connector setup", 13, MUTED
    )
    s.rect(48, 209, 864, 172, "E6F0F0", radius=12)
    s.text(64, 219, 820, "ONE SPARK APPLICATION / TWO STREAMS", 12, TEAL, True)
    for x, title, body in [
        (64, "Bronze", "Raw events\nAppend-only Parquet"),
        (282, "Silver", "Current state\nDelta upserts / deletes"),
        (500, "Gold", "Facts + dimensions\nHistorical-state joins"),
        (718, "Publication", "Completed Delta versions\nConsistent query view"),
    ]:
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
        "Recovery and metrics",
        "Checkpoints • progress • event logs",
        size=12,
    )
    s.card(
        344,
        406,
        272,
        64,
        "Invalid events",
        "Quarantine • publication withheld",
        size=12,
    )
    s.card(
        640, 406, 272, 64, "Spark SQL CLI", "Read tables at published versions", size=12
    )
    s.line(807, 349, 807, 406, arrow=True)

    s = new(
        "How data becomes useful",
        "DATA PROCESSING",
        ("app/gold.py", "app/materialize.py"),
        "Bronze preserves Kafka bytes and coordinates. Silver orders changes using PostgreSQL LSN and Kafka offset, conditionally merges the latest key state and retains delete guards. A new item joins a stored order; a changed parent refreshes stored children even without item events. Gold separates order/item fact grains and customer/product/date dimensions. Dimensions are SCD1. History means snapshot plus accumulated current state, not SCD2. The CLI reads the completed versions from a single manifest; transactions split across topics are eventually reconciled.",
    )
    s.card(
        48,
        120,
        272,
        208,
        "1 · Keep the event",
        "Bronze: original Kafka bytes\n\nSilver: latest state per key\n\nOrdered updates and deletes",
        size=17,
    )
    s.card(
        344,
        120,
        272,
        208,
        "2 · Use stored history",
        "New item → stored order\n\nChanged order → stored items\n\nEarlier batches stay available",
        size=17,
    )
    s.card(
        640,
        120,
        272,
        208,
        "3 · Build query tables",
        "One fact row per order\nOne fact row per item\n\nCustomer, product and date dimensions",
        size=17,
    )
    s.rect(48, 353, 864, 97, MINT, radius=10)
    s.text(66, 366, 826, "Consistent reads", 18, TEAL, True)
    s.text(
        66,
        399,
        826,
        "Readers see a completed publication. History = snapshot + stored current state.",
        18,
        INK,
    )

    s = new(
        "The local Spark size and its limits",
        "SPARK SIZING",
        ("docs/performance.md", "docker-compose.yaml"),
        "local[2] is two task slots in one driver JVM, not two executors. Both streams share them. Heap is 2 GiB within a 3 GiB container budget. Four shuffle partitions are an initial setting, not a proven optimum. Kafka intake is 20000 offsets per ten-second trigger; Bronze intake is 32 files per batch. These do not bound fanout or retained-target scans. Production needs actual rates/bytes, mutation mix, historical targets, storage, fanout and freshness/recovery goals. Shared durable storage and distributed measurements are still required.",
    )
    s.stat(
        48, 119, 272, "2 slots", "Shared by both streams", "local[2] • two-CPU quota"
    )
    s.stat(344, 119, 272, "2 GiB", "Spark heap", "3 GiB container limit")
    s.stat(
        640,
        119,
        272,
        "4",
        "Shuffle partitions",
        "Initial setting, not a proven optimum",
    )
    s.table(
        48,
        253,
        [235, 290, 339],
        ["Setting", "Local choice", "Why it matters"],
        [
            [
                "Streaming trigger",
                "Every 10 seconds",
                "Update frequency vs. repeated job cost",
            ],
            [
                "Kafka intake cap",
                "20,000 events per trigger",
                "Roughly 2,000 events/s at this cadence",
            ],
            [
                "Scale decisions",
                "Measure CPU, spill and lag",
                "Add capacity or reduce the expensive work",
            ],
        ],
        size=15,
        rowh=58,
    )
    s.text(
        48,
        467,
        864,
        "Local sizing was exercised. A production cluster size has not been validated.",
        16,
        TEAL,
        True,
    )

    s = new(
        "Which performance metrics were measured",
        "MEASUREMENT",
        ("docs/performance-stress.md", "docs/evidence/stress/streaming-levels.json"),
        "Actual arrival rate includes SQL execution and sampling pauses. Batch duration is active processing; freshness includes previous waiting. Each age p95 is calculated inside one batch. Its median/maximum across batches is not a global event p95. Native Spark metrics include CPU, task duration, GC, shuffle, spill and failures. Each stress scenario ran once, with arrival steps of about two minutes. Thirty seconds is an experimental reference, not a numeric SLA supplied by Deel.",
    )
    s.table(
        48,
        117,
        [210, 344, 310],
        ["Metric", "Simple meaning", "What it tells us"],
        [
            [
                "Actual input rate",
                "Source events received per second",
                "How much load was really generated",
            ],
            [
                "Batch duration",
                "Time to process a group of events",
                "How much work each batch takes",
            ],
            [
                "Freshness and backlog",
                "Time until publication; events waiting",
                "Whether processing keeps up",
            ],
            [
                "CPU, GC and spill",
                "Compute use, memory cleanup and disk spill",
                "Where resource pressure appears",
            ],
            [
                "Failures and counts",
                "Failed tasks and source/Gold agreement",
                "Whether results remain correct",
            ],
        ],
        size=15,
        rowh=53,
    )
    s.text(
        48,
        461,
        864,
        "Separate tests: bulk load, short CDC steps, broker recovery and query concurrency.",
        15,
        TEAL,
        True,
    )

    big = max(
        (r for r in d["materialization"] if not r["hot"]), key=lambda r: r["events"]
    )
    s = new(
        "Large loads: Gold takes most of the time",
        "PERFORMANCE RESULTS",
        (
            "docs/evidence/stress/materialization.json",
            "docs/evidence/stress/scale-1m-spark-metrics.json",
        ),
        "Initial fixtures contain about 0.6, 3 and 6 million events. Materialization excludes Spark initialization, separate Bronze writing and later checks. The largest took 370.5 seconds; Gold used 232.5 seconds, about 63%. Task disk spill of 846 MB, CPU of 482.8s and GC of 51.3s are totals for the whole largest application, including update/checks. Task sums are not wall time or JVM-wide GC percentages. Failed attempts were zero. The memory probe approached the container cap without OOM. Bulk events/s does not certify streaming freshness.",
    )
    s.image(assets / "materialization.png", 48, 118, 595, 314)
    s.stat(
        665,
        119,
        247,
        f"{big['initial_seconds']:.1f}s",
        "6 million input events",
        "Initial processing time",
    )
    s.stat(665, 247, 247, "63%", "Time spent in Gold", "232.5s of 370.5s")
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
        "Largest run: 482.8s task CPU • 51.3s task GC. Next focus: Gold work and target-table scans.",
        15,
        TEAL,
        True,
    )

    s = new(
        "Streaming: more input means more delay",
        "PERFORMANCE RESULTS",
        (
            "docs/evidence/stress/streaming-levels.json",
            "docs/evidence/stress/streaming-batches.json",
        ),
        "Use measured input rates: 235, 954, 2356 and 2587 events/s. The nominal 6000/s target was not reached. The chart shows median/highest within-batch p95 ages, not global event p95. Above roughly 2000/s the original cap contributes growing lag. Even the lowest step had a batch p95 above the 30s experimental reference. Highest batch p95 was 112.5s. All steps drained after input stopped; the highest two needed about 66s and 70s. These short tests do not certify a production SLA.",
    )
    s.image(assets / "freshness.png", 48, 117, 595, 314)
    s.card(
        665,
        119,
        247,
        143,
        "Delay grows with load",
        "Above ~2,000 events/s, waiting events increased.\nHighest batch p95: 112.5s.",
        size=16,
        accent=GOLD,
    )
    s.card(
        665,
        283,
        247,
        143,
        "The pipeline caught up",
        "All four steps drained after input stopped.\nHighest steps: ~66s / ~70s.",
        size=16,
    )
    s.text(
        48,
        452,
        864,
        "p95 covers 95% of a batch's events, not all events globally. Steps ~2 min; 6k/s was not reached.",
        14,
        MUTED,
    )

    s = new(
        "Correctness and recovery were checked",
        "VALIDATION RESULTS",
        (
            "docs/evidence/stress/final-integrity.json",
            "docs/evidence/stress/broker-outage.jsonl",
        ),
        "774001 counts raw events, not unique orders. Topic offsets, physical Bronze rows/distinct Kafka coordinates and published input counts matched. Four business metrics matched independent PostgreSQL queries three times: after load, after mutations and after recovery. We applied 12375 order updates and 6378 item deletes. While Kafka was stopped, 6000 events committed to the source. The 53.5s interval includes stop, wait, restart, processing and polling. Spark needed no restart. This demonstrates recovery with retained volumes, not single-broker HA or disk-loss recovery.",
    )
    s.stat(
        48,
        118,
        272,
        "774,001",
        "Raw events accounted for",
        "Kafka = Bronze = published input",
    )
    s.stat(
        344, 118, 272, "3 / 3", "Source comparisons passed", "All four business metrics"
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
        264,
        418,
        166,
        "Updates and deletes",
        "12,375 order updates\n6,378 item deletes\nSource and Gold results matched.",
        size=18,
    )
    s.card(
        490,
        264,
        422,
        166,
        "Temporary Kafka outage",
        "6,000 events written while offline\nBroker restarted; events recovered\nSpark restart was not needed.",
        size=18,
    )
    s.text(
        48,
        458,
        864,
        "Recovery used retained volumes and checkpoints. One broker is not a highly available setup.",
        15,
        TEAL,
        True,
    )

    s = new(
        "Delivery summary and next step",
        "HANDOVER",
        ("README.md", "docs/presentation/README.md"),
        "Close with a working source-to-query platform, measured evidence and a clear next experiment. Prioritize Gold target scans/merge work, then compare resources and file layout using real arrival/freshness targets. Repository contents include application, SQL, Docker, tests, experiment code and slide sources. The CLI reads completed publications. The README indexes the deliverables. Detailed explanations remain in documentation and speaker notes; there is no slide appendix.",
    )
    s.card(
        48,
        118,
        272,
        174,
        "Delivered",
        "Streaming pipeline\nDelta dimensional tables\nCLI + code + tests + evidence",
        size=17,
    )
    s.card(
        344,
        118,
        272,
        174,
        "Measured",
        "Large loads completed\nGold is the main cost\nFreshness worsens under load",
        size=17,
    )
    s.card(
        640,
        118,
        272,
        174,
        "Next",
        "Reduce Gold work\nTest real workload and targets\nThen size production resources",
        size=17,
    )
    s.rect(48, 318, 864, 70, NAVY, radius=10)
    s.text(
        68,
        340,
        824,
        "docker compose run --rm --no-deps query metrics all",
        19,
        "6DE0C6",
    )
    s.text(48, 416, 864, "Code, run instructions and evidence", 19, INK, True)
    s.text(
        48,
        452,
        864,
        "github.com/fabdutra/deel-data-engineering-task",
        19,
        TEAL,
        True,
        link=REPO,
    )
    return slides
