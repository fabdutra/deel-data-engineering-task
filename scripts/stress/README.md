# Underlying stress-test work

This is the measurement harness used for the [recorded stress analysis](../../docs/performance-stress.md). It is supplied for methodology review and as the underlying code for the presentation's findings. Repository/output paths were relocated, and the previously manual offline event-log export was automated; instrumentation, workloads, rates and mutation logic are preserved. A fresh-environment launcher was added. These relocated scripts were syntax-checked; the complete heavy campaign was not rerun after relocation.

| Source | Role |
|---|---|
| `workload.py`, `instrument.py`, `run.sh` | Invoke original application transformations; measure per-table Silver MERGE, Gold refresh, publication and source-event age; generate bulk and hot-parent fixtures |
| `controller.py` | Run 100k/500k/1m-order fixtures and the million-child parent case; sample Docker resources; restart failed cases |
| `bronze_controller.py` | Produce two payload sizes, compare 20k/200k intake caps with a retained checkpoint, verify raw coordinates |
| `online_controller.py` | Source SQL generation, stepped nominal rates, independent source reconciliation, mutations, broker outage/restart, query concurrency |
| `finish.py` | Compare final source/Gold aggregates, offsets and physical raw uniqueness; retain evidence and stop stress services |
| `analyze.py`, `postprocess.py` | Parse actual logs/resources and call repository event-log/progress extractors |
| `compose.yaml` | Mount the test-only harness and isolate the online lake; inherit the application image and budgets |
| `run_campaign.py` | Refuse existing campaign resources/running original stack, then run phases explicitly |

## Prerequisites and execution

The fixed project/container names are `deel-stress*`. Use a **fresh isolated Docker environment**, sufficient disk space (allow at least 20 GB free for fixtures/copies/images) and approximately 8 GB VM RAM. Runs can take tens of minutes, consume substantial CPU/disk, commit synthetic source mutations, and deliberately stop/restart the isolated Kafka broker. The scripts do not delete existing volumes or reset checkpoints. A failure may retain running services for diagnosis.

From repository root:

```sh
python3 scripts/setup.py
# Build images without starting the original assessment services:
docker compose build transactions-db pipeline
# Check prerequisites; no containers started by this command:
python3 scripts/stress/run_campaign.py --check
# Explicitly run the heavy campaign on the fresh environment:
python3 scripts/stress/run_campaign.py
```

The source image name follows the root Compose project: `deel-assessment-transactions-db:latest`. Do not change the Compose project identity without updating the historical harness's fixed names/network. Docker images and package downloads require network access during initial setup; credentials stay in the ignored `.env` and Connect provider.

Outputs under `results/` and `logs/` are ignored. If a run fails, inspect logs first; do not rerun source generation on the partially populated lake as an independent benchmark. To stop only this campaign without deleting data:

```sh
docker compose -p deel-stress -f docker-compose.yaml -f scripts/stress/compose.yaml stop
```

The original recorded campaign's initial snapshot was delivered twice, adding 185 raw events above its planned counter. This is not guaranteed to repeat. The final physical offsets/row checks and source business queries are the independent correctness checks. Timings, snapshots and output rows will differ in a fresh execution.

For presentation rebuilding, use the already recorded JSON under `docs/evidence/stress/`; rerunning this campaign is **not** required. Figures are computed by [the presentation builder](../presentation/build.py), and per-slide input hashes are indexed in [the manifest](../../docs/presentation/manifest.json). The two-minute load steps, source sampling pauses and within-batch p95 methodology are explained in the stress report; they do not establish an event-global SLA.
