# Access Modernization Pipeline Validation

## Overview

This prototype demonstrates how a representative Microsoft Access reporting workflow can be translated into a governed Databricks lakehouse pipeline.

The workflow preserves familiar enterprise patterns:

- source ingestion
- business-rule transformation
- exception handling
- control-total reconciliation
- reporting-ready aggregation
- scheduled or manual orchestration

## Pipeline

The Lakeflow Jobs workflow runs three dependent notebook tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_reporting`

Execution order:

Bronze ingestion -> Silver transformation -> Gold reporting

## Source datasets

Two synthetic Access-style CSV exports were used:

- `party_master_export.csv`
- `monthly_reporting_export.csv`

The datasets intentionally include:

- two internal parties sharing one LEI
- a missing party name
- an invalid LEI
- a superseded monthly submission
- reporting rows that cannot join to the trusted party master

## Bronze validation

| Dataset | Rows |
|---|---:|
| Party master | 7 |
| Monthly reporting | 9 |

Bronze preserves the original source values and adds ingestion lineage metadata.

## Silver validation

### Party master

| Outcome | Rows |
|---|---:|
| Trusted | 5 |
| Quarantined | 2 |
| Reconciled | 7 |

### Monthly reporting

| Outcome | Rows |
|---|---:|
| Trusted | 5 |
| Quarantined | 3 |
| Superseded submission audit | 1 |
| Reconciled | 9 |

The repeated LEI is not removed because the party-master grain is one row per internal party ID. Multiple parties may legitimately map to the same legal entity.

For repeated monthly submissions, the latest `source_updated_at` record is retained and the earlier record is preserved in an audit table.

## Gold validation

| Dataset | Rows |
|---|---:|
| Gold party reporting | 5 |
| Gold analyst summary | 3 |
| Gold portfolio summary | 1 |
| Gold quality metrics | 8 |

Gold outputs include:

- reporting detail by month and party
- analyst-level exposure and exception summaries
- portfolio-level control totals
- pipeline quality metrics
- review-priority classifications
- reporting-readiness indicators

## Lakeflow Jobs run evidence

The manually launched pipeline completed successfully.

| Task | Status | Duration |
|---|---|---:|
| `bronze_ingestion` | Succeeded | 28s |
| `silver_transformation` | Succeeded | 37s |
| `gold_reporting` | Succeeded | 30s |

Total job duration: **1m 36s**

Additional run observations:

- Serverless compute was used.
- Each downstream task waited for its dependency.
- Databricks reported 7 upstream tables and 7 downstream tables.
- The job run exposed Graph, Timeline, List, task duration, status, lineage, and run-event views.

## Modernization interpretation

The prototype does not attempt a line-by-line rewrite of Access queries, macros, or VBA.

Instead, it separates the workflow into:

- raw source preservation
- standardized and tested transformation logic
- explicit quarantine and audit outcomes
- reusable reporting tables
- observable orchestration

This retains the original business purpose while improving governance, traceability, scalability, and operational visibility.

## What This Demonstrates

This representative prototype translates legacy Microsoft Access-style operational reporting workflows into Databricks and Lakeflow patterns. It ingests representative exports into Bronze Delta tables; applies validation, deduplication, referential-integrity, quarantine, and exception-handling rules in Silver; and produces Power BI-ready reporting outputs in Gold. The dependent notebooks are orchestrated through Lakeflow Jobs, with row-level reconciliation, task dependencies, and run observability validated across the pipeline.

## Downstream Reporting Validation

- [Power BI dashboard validation](power-bi-results.md)