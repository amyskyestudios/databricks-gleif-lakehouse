# GLEIF Lakehouse Architecture

## Overview

This project implements a small but complete Databricks lakehouse workflow
using Python, pandas, PySpark, Spark SQL, Delta Lake, and Lakeflow Jobs.

The architecture separates raw ingestion, trusted transformation,
business-ready analytics, and operational auditing.

```mermaid
flowchart LR
    A[GLEIF CSV Source] --> B[Bronze Ingestion]
    B --> C[(bronze_gleif_entities)]

    C --> D[Silver Transformation]
    D --> E[(silver_gleif_entities)]
    D --> F[(quarantine)]
    D --> G[(duplicate audit)]

    E --> H[Gold Analytics]
    H --> I[(entity reporting)]
    H --> J[(status summary)]
    H --> K[(quality metrics)]

    I --> L[Delta History Audit]
    J --> L
    K --> L

    M[Lakeflow Job] -. orchestrates .-> B
    M -. orchestrates .-> D
    M -. orchestrates .-> H
    M -. orchestrates .-> L
cat > docs/architecture.md <<'EOF'
# GLEIF Lakehouse Architecture

## Overview

This project implements a small but complete Databricks lakehouse workflow
using Python, pandas, PySpark, Spark SQL, Delta Lake, and Lakeflow Jobs.

The architecture separates raw ingestion, trusted transformation,
business-ready analytics, and operational auditing.

```mermaid
flowchart LR
    A[GLEIF CSV Source] --> B[Bronze Ingestion]
    B --> C[(bronze_gleif_entities)]

    C --> D[Silver Transformation]
    D --> E[(silver_gleif_entities)]
    D --> F[(quarantine)]
    D --> G[(duplicate audit)]

    E --> H[Gold Analytics]
    H --> I[(entity reporting)]
    H --> J[(status summary)]
    H --> K[(quality metrics)]

    I --> L[Delta History Audit]
    J --> L
    K --> L

    M[Lakeflow Job] -. orchestrates .-> B
    M -. orchestrates .-> D
    M -. orchestrates .-> H
    M -. orchestrates .-> L
```

## Technology Responsibilities

| Technology | Responsibility |
|---|---|
| Databricks | Workspace, serverless compute, catalog, notebooks, and job orchestration |
| Python | General notebook control flow and configuration |
| pandas | Small local CSV ingestion for the demonstration source |
| PySpark | Distributed DataFrame transformations and metadata processing |
| Spark SQL | Table creation, aggregations, validation, and metadata queries |
| Delta Lake | Transactional tables, history, versions, and reliable persisted outputs |
| Lakeflow Jobs | Task dependencies, execution order, runtime monitoring, and concurrency control |
| GitHub | Source control, documentation, and portable project evidence |

## Medallion Translation

- Bronze = raw / staging
- Silver = standardized / intermediate
- Gold = business-ready / reporting mart

## Business Grain

The demonstration identifies repeated LEIs for duplicate auditing.

In a production party-data model, repeated LEIs must not automatically be
deleted. Multiple internal party records may legitimately map to the same
legal entity.

A production design would therefore commonly separate:

1. Legal entity records at one row per LEI
2. Internal party records at one row per party identifier
3. A party-to-LEI relationship table

Data-quality and deduplication rules must follow the intended business grain.

## Orchestrated Execution

The Lakeflow Job runs:

1. Bronze ingestion
2. Silver transformation
3. Gold analytics
4. Delta history audit

Each task runs only after its upstream dependency succeeds.

The job uses serverless autoscaling compute, queueing, and a maximum of one
concurrent run to prevent overlapping pipeline executions.
