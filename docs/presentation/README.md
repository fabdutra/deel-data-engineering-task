# ACME Delivery assessment presentation

Professional presentation for the Deel Senior Data Engineer assessment and follow-up discussion. Author: Fabricio Dutra. Language: English. Format: 16:9, **15 main slides plus 7 technical appendix slides**, approximately 18–20 minutes for the main narrative.

## Submission files

- [PDF presentation](acme-delivery-assessment.pdf): ready to attach or present.
- [Editable PowerPoint](acme-delivery-assessment.pptx): editable text, architecture/model diagrams and tables; evidence charts are embedded images generated from recorded data. English speaker notes are included.
- [Slide outline and English speaker notes](slides.md): readable narrative and direct evidence links.
- [Source and output manifest](manifest.json): SHA-256 hashes of presentation inputs and delivered PDF/PPTX.
- [Source generator](../../scripts/presentation/build.py), [slide content/layout](../../scripts/presentation/deck.py) and [pinned dependencies](../../scripts/presentation/requirements.txt).

The private repository contains all underlying application code, SQL, Docker configuration, tests and retained analysis. Start at [the root reviewer guide](../../README.md#reviewer-guide-deliverables-and-evidence), [requirement mapping](../assessment-deliverables.md), [evidence index](../evidence/README.md) and [measurement harness](../../scripts/stress/README.md). Personal interview notes are excluded.

## Methodology and findings

The narrative moves from the four operational questions to explicit assumptions, architecture, historical enrichment, dimensional grains, ordering/deletion semantics, the publication contract, local resource budgets and independent validation. It distinguishes measured facts from proposed production changes.

Charts read the retained [materialization](../evidence/stress/materialization.json) and [stepped CDC measurements](../evidence/stress/streaming-levels.json). The presentation separates bulk execution rate, source arrival rate, streaming batch duration, source-to-publication age and CLI cold start. Batch p95 summaries are not presented as global event p95. The nominal 6k/s source target was not attained; the actual highest step was approximately 2.59k/s. No certified production SLA or fixed production executor count is claimed.

The data-results appendix contains selected rows from [actual query output](../evidence/stress/query-output.json), with its publication timestamp. It is a formatted presentation of captured data, not a Spark UI screenshot. The full results and independent PostgreSQL reconciliations remain available in the evidence directory.

## Rebuild without Docker or live credentials

Use Python 3.9–3.12 with the pinned packages (the delivered build used 3.9); initial package installation requires internet access. Run from repository root:

```sh
python3 -m venv /tmp/deel-presentation-venv
/tmp/deel-presentation-venv/bin/python -m pip install -r scripts/presentation/requirements.txt
/tmp/deel-presentation-venv/bin/python scripts/presentation/build.py
/tmp/deel-presentation-venv/bin/python scripts/presentation/validate.py
```

The builder reads repository evidence only. It does not access `.env`, personal context, live source databases or GitHub credentials; it does not start Docker or rerun performance tests. It generates both formats from the same slide geometry. A portable font fallback uses installed Arial, Liberation Sans or Matplotlib's DejaVu Sans; font substitution can change line wrapping and binary hashes. PDF/PPTX metadata can also change binary hashes between builds; the manifest identifies that build's inputs/outputs.

After committing the reviewed deliverables, optionally create a local source/evidence bundle:

```sh
python3 scripts/presentation/package_submission.py
```

The ZIP is written to ignored `artifacts/deel-assessment-submission.zip`. It includes tracked files and a hash manifest, excluding credentials, personal context and Git internals. On systems with multiple Git installations, pass `--git /path/to/git` if needed. See the [submission checklist and email draft](../submission.md).

## Suggested live walkthrough

1. Present the 15 main slides, using the appendix for questions.
2. Open the root README, show the reviewer index and recorded CLI/query output.
3. If demonstrating live, follow the root run instructions, wait for a successful publication, then query all four metrics and point out the publication timestamp.
4. Use evidence to discuss the observed constraints rather than rerunning large workloads during the interview.

The presentation summarizes the measured implementation; it does not replace the detailed architecture, performance analysis or test evidence.
