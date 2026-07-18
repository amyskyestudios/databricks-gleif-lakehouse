# Databricks GLEIF Lakehouse & Access Modernization Lab

An end-to-end data-engineering portfolio project using Databricks, PySpark,
Spark SQL, Delta Lake, Lakeflow Jobs, Python, and pandas.

The repository contains two completed, production-inspired workflows:

1. A GLEIF legal-entity lakehouse pipeline
2. A representative Microsoft Access modernization pipeline

Together, the workflows demonstrate layered lakehouse processing,
data-quality controls, business-grain analysis, exception handling,
analytical dataset delivery, Delta auditing, and workflow orchestration.

## Project Status

✅ GLEIF Bronze, Silver, Gold, Delta audit, and Lakeflow workflow completed

✅ Access modernization Bronze, Silver, Gold, and Lakeflow workflow completed

🔜 Power BI semantic modeling, dashboard development, and DAX measures

---

# Workflow 1: GLEIF Legal-Entity Lakehouse

This workflow processes a controlled sample of Global Legal Entity Identifier
Foundation reference data.

## GLEIF Architecture

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

## GLEIF Processing Layers

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

## GLEIF Lakeflow Job

The workflow runs four dependent notebook tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_analytics`
4. `delta_history_audit`

Each task runs only after its upstream dependency succeeds.

The job uses serverless autoscaling compute, queueing, and a maximum of one
concurrent run to prevent overlapping executions.

A validated manual run completed successfully in approximately 1 minute
46 seconds.

## GLEIF Validation Results

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

## GLEIF Business-Grain Consideration

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

## GLEIF Notebooks

- [Exploration](notebooks/00_gleif_entity_matching_exploration.py)
- [Bronze ingestion](notebooks/01_bronze_ingestion.py)
- [Silver transformation](notebooks/02_silver_transformation.py)
- [Gold analytics](notebooks/03_gold_analytics.py)
- [Delta history audit](notebooks/04_delta_history_audit.py)

---

# Workflow 2: Microsoft Access Modernization Prototype

This workflow demonstrates how representative Microsoft Access reporting
patterns can be translated into a governed Databricks lakehouse pipeline.

It is not presented as a production migration. It is a focused prototype used
to validate how established Access, SQL, ETL, reporting, and operational-control
experience maps into Databricks and Lakeflow patterns.

## Access Modernization Architecture

    Access-style CSV exports
              |
              v
       Bronze ingestion
              |
              v
      Silver transformation
       |        |        |
       v        v        v
    Trusted  Quarantine  Audit
              |
              v
        Gold reporting
              |
              v
       Lakeflow Jobs
              |
              v
       Power BI-ready data

## Representative Sources

- `data/sample/access_modernization/party_master_export.csv`
- `data/sample/access_modernization/monthly_reporting_export.csv`

The synthetic datasets intentionally include:

- two internal parties sharing one LEI
- a missing party name
- a malformed LEI
- two submissions for the same monthly reporting business key
- reporting records that cannot join to the trusted party master

## Access Bronze Layer

Bronze preserves the exported values and adds:

- source-file metadata
- source-system metadata
- ingestion timestamps

Source counts:

| Dataset | Rows |
|---|---:|
| Party master | 7 |
| Monthly reporting | 9 |

## Access Silver Layer

Silver replaces the kinds of chained validation, totals, exception, and
latest-record queries often implemented in Access.

It performs:

- field trimming and standardization
- date, timestamp, integer, and decimal conversion
- party-master validation
- quarantine routing with explicit reasons
- latest-submission selection
- superseded-submission preservation
- referential-integrity validation
- source-to-outcome reconciliation

### Party-Master Reconciliation

| Outcome | Rows |
|---|---:|
| Trusted | 5 |
| Quarantined | 2 |
| Reconciled | 7 |

### Monthly-Reporting Reconciliation

| Outcome | Rows |
|---|---:|
| Trusted | 5 |
| Quarantined | 3 |
| Superseded-submission audit | 1 |
| Reconciled | 9 |

Two distinct party IDs sharing one LEI remain valid because the party-master
grain is one row per internal party, not one row per legal entity.

For repeated monthly submissions, the newest `source_updated_at` record is
retained while the older record remains available for audit.

## Access Gold Layer

The Gold layer produces:

- party-level reporting detail
- analyst-level exposure and exception summaries
- portfolio-level control totals
- pipeline quality metrics
- review-priority classifications
- reporting-readiness indicators

Validated Gold counts:

| Dataset | Rows |
|---|---:|
| Gold party reporting | 5 |
| Gold analyst summary | 3 |
| Gold portfolio summary | 1 |
| Gold quality metrics | 8 |

## Access Modernization Lakeflow Job

The workflow runs three dependent notebook tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_reporting`

A manually launched serverless run completed successfully:

| Task | Status | Duration |
|---|---|---:|
| Bronze ingestion | Succeeded | 28 seconds |
| Silver transformation | Succeeded | 37 seconds |
| Gold reporting | Succeeded | 30 seconds |
| Total workflow | Succeeded | 1 minute 36 seconds |

Databricks reported seven upstream tables and seven downstream tables for the
successful run.

## Access Modernization Notebooks

- [Bronze ingestion](notebooks/05_access_bronze_ingestion.py)
- [Silver transformation](notebooks/06_access_silver_transformation.py)
- [Gold reporting](notebooks/07_access_gold_reporting.py)

## Access Modernization References

- [Access modernization mapping](docs/access-modernization-mapping.md)
- [Access pipeline validation](docs/validation/access-modernization-results.md)

---

# Technology Stack

- Databricks
- Lakeflow Jobs
- PySpark
- Spark SQL
- Delta Lake
- Python
- pandas
- Git
- GitHub
- Microsoft Access modernization concepts
- Power BI and DAX planned as the next reporting layer

# Repository Guide

| Path | Purpose |
|---|---|
| `data/sample/` | Controlled source datasets |
| `notebooks/` | Databricks notebook source files |
| `docs/architecture.md` | Lakehouse architecture and technology responsibilities |
| `docs/access-modernization-mapping.md` | Access-to-Databricks translation reference |
| `docs/validation/` | Execution, reconciliation, and workflow evidence |

# Legacy-to-Lakehouse Translation

The architecture translates familiar enterprise data-processing patterns into
the Databricks ecosystem:

- Raw tables and staging loads → Bronze
- Intermediate queries and transformation tables → Silver
- Reporting tables and marts → Gold
- Exception reports → Quarantine and audit tables
- Stacked SQL criteria logic → PySpark and Spark SQL business rules
- Manual control totals → Automated reconciliation metrics
- Scheduled process chains → Lakeflow Job dependencies
- Custom runtime logs → Databricks graph, timeline, and task history

# Interview Summary

I built two end-to-end Databricks workflows to deepen and demonstrate my
lakehouse engineering skills.

The first processes GLEIF legal-entity reference data through Bronze, Silver,
and Gold Delta tables and includes quality controls, reconciliation, Delta
history inspection, and Lakeflow orchestration.

The second translates representative Microsoft Access reporting patterns into
Databricks. It preserves source exports in Bronze, implements validation,
deduplication, referential integrity, quarantine, and audit handling in Silver,
and creates reporting-ready Gold datasets for downstream Power BI analytics.

Both workflows are stored in GitHub with architecture notes, validation
results, implementation details, and concise reference material for technical
discussion.

# Next Chapter

The next milestone is to connect the Access modernization Gold outputs to
Power BI and build:

- a reporting model
- executive and operational visuals
- reusable DAX measures
- documented validation between Databricks and Power BI
