# Submission package

The recruiter requested a professional presentation and all underlying work/code. The engineering assessment's implementation and evidence remain in the private repository; the presentation summarizes that work for the next interview.

| Requested item | Delivered artifact |
|---|---|
| Professional presentation | [PDF](presentation/acme-delivery-assessment.pdf) and [editable PowerPoint](presentation/acme-delivery-assessment.pptx) |
| Methodology, assumptions and analytical approach | Main slides 2–10, [English speaker notes](presentation/slides.md), [architecture](architecture.md) and [measurement analysis](performance-stress.md) |
| Findings and limitations | Main slides 11–14 and technical appendix; evidence-linked performance, freshness, correctness and recovery results |
| Underlying implementation | [PySpark application](../app/), [SQL](../sql/), [Docker stack](../docker-compose.yaml), [tests](../tests/) |
| Underlying experimental/analysis work | [Measurement harness](../scripts/stress/README.md), [recorded data](evidence/stress/), [slide/figure generator](../scripts/presentation/), [input/output hashes](presentation/manifest.json) |
| Run/review instructions | [Root README](../README.md), [presentation guide](presentation/README.md), [evidence index](evidence/README.md) |

The private delivery is [fabdutra/deel-data-engineering-task](https://github.com/fabdutra/deel-data-engineering-task). The designated reviewers were invited separately; recipients must accept repository invitations. The repository preserves the starter/development history. The downloadable local ZIP is a snapshot; the Git history is available in GitHub.

## Suggested email text

The following is a draft only; no email is sent by this repository or its scripts.

> Hi Adriana,
>
> I have completed the Senior Data Engineer take-home assessment. The presentation is attached in PDF and PowerPoint formats.
>
> The implementation, SQL, Docker setup, tests, underlying performance-test code, recorded evidence and presentation source are available in the private repository:
> https://github.com/fabdutra/deel-data-engineering-task
>
> The README includes a reviewer guide linking each deliverable. Repository invitations have been sent to `juan-domingos-deel` and `xi-wang-deel`.
>
> The presentation covers the architecture, assumptions, Spark sizing approach, correctness/recovery validation and measured performance findings, including the limitations of the local tests. I look forward to presenting and discussing the project in the next round.
>
> Best regards,
>
> Fabricio Dutra

Attach `acme-delivery-assessment.pdf` and, if useful, `acme-delivery-assessment.pptx`. A local source/evidence ZIP can be generated with the packaging command in the presentation guide. No credentials, personal preparation notes or large raw lake fixtures belong in the submission.
