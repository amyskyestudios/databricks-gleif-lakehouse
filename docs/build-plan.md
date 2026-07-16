# Databricks GLEIF Lakehouse Build Plan

## Objective

Build a practical Databricks and PySpark lakehouse pipeline using controlled GLEIF reference data.

The project will demonstrate:

- Databricks notebook development
- PySpark ingestion and transformation
- Delta Lake tables
- Bronze, Silver, and Gold architecture
- SQL-based analytics
- Data-quality validation
- Reconciliation and execution evidence
- Git and GitHub documentation

## Milestone 01: Bronze Ingestion

- Load a controlled GLEIF sample dataset
- Preserve raw source fields
- Add ingestion metadata
- Validate schema and record counts
- Write the result as a Bronze Delta table

## Milestone 02: Silver Transformation

- Standardize column names and data types
- Trim and normalize text values
- Identify null and duplicate records
- Apply business-rule validations
- Write validated records as a Silver Delta table

## Milestone 03: Gold Analytics

- Produce curated entity-level datasets
- Create operational data-quality metrics
- Query results using Databricks SQL
- Prepare a reporting-ready output suitable for Power BI

## Evidence to Publish

- Exported Databricks notebooks
- Architecture diagram
- Validation results
- Screenshots of successful execution
- Transformation and data-dictionary documentation
- Interview-ready project summary
