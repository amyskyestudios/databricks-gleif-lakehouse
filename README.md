# Databricks GLEIF Lakehouse

An active data-engineering project using Databricks, PySpark, and GLEIF
reference data to demonstrate layered lakehouse processing, data-quality
validation, and analytical dataset delivery.

## Project Status

🚧 Active build — initial ingestion and transformation notebooks are in development.

## Planned Architecture

- Bronze: Raw GLEIF data ingestion and source metadata
- Silver: Cleansing, standardization, deduplication, and validation
- Gold: Analytical entity datasets and operational quality metrics
- Data quality: Null, uniqueness, schema, record-count, and business-rule checks
- Documentation: Architecture, transformation logic, and execution evidence

## Planned Technologies

- Databricks
- PySpark
- Delta Lake
- SQL
- Python
- Git and GitHub

## Current Focus

The first implementation milestone will deliver a working Bronze-to-Silver
pipeline using a controlled GLEIF sample dataset.
