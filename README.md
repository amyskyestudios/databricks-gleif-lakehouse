# Databricks GLEIF Lakehouse

An end-to-end data-engineering project using Databricks, PySpark, Spark SQL,
Delta Lake, and GLEIF reference data.

The project demonstrates layered lakehouse processing, data-quality controls,
business-grain analysis, analytical dataset delivery, Delta auditing, and
workflow orchestration.

## Project Status

✅ Working end-to-end implementation

The Bronze, Silver, Gold, and audit notebooks have been executed successfully
in Databricks and orchestrated as a four-task Lakeflow Job.

## Architecture

    GLEIF CSV source
           |
           v
    Bronze ingestion
           |
           v
    Silver transformation
       |          |
       v          v
    Quarantine   Duplicate audit
           |
           v
    Gold analytics
           |
           v
    Delta history audit

Detailed architecture and technology responsibilities:

- [Lakehouse architecture](docs/architecture.md)

## Processing Layers

### Bronze — Raw / Staging

- Ingests the controlled GLEIF sample
- Preserves source values and quality issues
- Adds source-system and ingestion metadata
- Persists the data as a Delta table

### Silver — Standardized / Intermediate

- Standardizes text and date values
- Applies validation and quality flags
- Separates trusted, quarantined, and duplicate-audit records
- Preserves questionable records for traceability rather than silently
  discarding them

### Gold — Business-Ready / Reporting Mart

Creates three analytical Delta tables:

- `gold_gleif_entity_reporting`
- `gold_gleif_status_summary`
- `gold_gleif_quality_metrics`

The Gold outputs provide entity-level reporting, status summaries, and
pipeline reconciliation metrics suitable for downstream BI consumption.

## Lakeflow Job

The Databricks job runs four dependent notebook tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_analytics`
4. `delta_history_audit`

Each task runs only after its upstream dependency succeeds.

The job uses serverless autoscaling compute, queueing, and a maximum of one
concurrent run to prevent overlapping executions.

A validated manual run completed successfully in approximately 1 minute
46 seconds.

## Validation Results

The controlled source contains six records:

| Result Category | Rows |
|---|---:|
| Trusted Silver records | 4 |
| Quarantined records | 1 |
| Duplicate audit records | 1 |
| Reconciled source rows | 6 |

The pipeline reconciles completely:

`4 trusted + 1 quarantined + 1 duplicate = 6 source rows`

Execution evidence:

- [Medallion validation results](docs/validation/medallion-results.md)
- [Lakeflow Job validation results](docs/validation/lakeflow-job-results.md)

## Business-Grain Consideration

Repeated LEIs are identified for demonstration and audit purposes, but a
repeated LEI is not automatically a business duplicate.

In a production party-data model, multiple internal party records may
legitimately map to the same legal entity. A production design would commonly
separate:

1. Legal entities at one row per LEI
2. Internal parties at one row per party identifier
3. Party-to-LEI relationships

Deduplication and quality rules must therefore follow the intended business
grain.

## Repository Guide

| Path | Purpose |
|---|---|
| `data/sample/` | Controlled GLEIF source data |
| `notebooks/01_bronze_ingestion.py` | Raw ingestion and source metadata |
| `notebooks/02_silver_transformation.py` | Standardization, validation, quarantine, and duplicate audit |
| `notebooks/03_gold_analytics.py` | Reporting-ready tables and reconciliation metrics |
| `notebooks/04_delta_history_audit.py` | Delta history and table-detail auditing |
| `docs/architecture.md` | Architecture and technology responsibilities |
| `docs/validation/` | Execution and reconciliation evidence |

## Technologies

- Databricks
- Lakeflow Jobs
- PySpark
- Spark SQL
- Delta Lake
- Python
- pandas
- Git and GitHub

## Legacy-to-Lakehouse Translation

The architecture translates familiar enterprise data-processing patterns into
the Databricks ecosystem:

- Raw tables and staging loads → Bronze
- Intermediate queries and transformation tables → Silver
- Reporting tables and marts → Gold
- Exception reports → Quarantine and audit tables
- Manual control totals → Automated reconciliation metrics
- Scheduled process chains → Lakeflow Job dependencies
- Custom runtime logs → Databricks graph, timeline, and task history
