# Access-to-Databricks Modernization Mapping

## Purpose

This document maps common Microsoft Access reporting and ETL patterns to a
modern Databricks lakehouse architecture.

The goal is not to dismiss the legacy solution. Access often delivered valuable
business automation quickly. Modernization preserves the business logic while
improving scale, governance, observability, reuse, and downstream analytics.

## Legacy-to-Modern Mapping

| Microsoft Access Pattern | Databricks / Modern Equivalent |
|---|---|
| Linked tables and imported files | Bronze ingestion tables |
| Make-table and append queries | PySpark or Spark SQL transformations |
| Update and delete queries | Delta Lake MERGE, UPDATE, and DELETE |
| Intermediate working tables | Silver Delta tables |
| Validation and exception queries | Quarantine and data-quality tables |
| VBA procedures | Python or PySpark notebook logic |
| Macros and form buttons | Lakeflow Job tasks and dependencies |
| Scheduled Access processes | Lakeflow Job triggers |
| Persistent reporting tables | Gold analytical tables |
| Manual control totals | Reconciliation metrics |
| Custom runtime log tables | Job run history, graph, timeline, and task logs |
| Excel report extracts | Power BI-ready Gold tables |
| Shared-drive database files | Governed catalog tables and Git-controlled code |

## Representative Legacy Workflow

A typical Access reporting process may:

1. Import one or more source files
2. Clear or refresh staging tables
3. Run chained transformation queries
4. Apply validation and exception checks
5. Create reporting tables
6. Export multiple Excel worksheets
7. Record row counts and completion timestamps

## Modernized Workflow

The equivalent Databricks design may:

1. Ingest source files into Bronze Delta tables
2. Standardize and validate records in Silver
3. Preserve rejected records in quarantine tables
4. Create business-ready Gold tables
5. Reconcile source and output row counts
6. Orchestrate notebook tasks with Lakeflow Jobs
7. Expose Gold tables for Power BI consumption
8. Use job history and Delta metadata for operational monitoring

## Modernization Method

Modernization should not begin by rewriting every Access component line by
line.

First identify:

- Business purpose
- Grain of each dataset
- Source-to-target dependencies
- Transformation rules
- Exception handling
- Control totals
- Reporting consumers
- Scheduling and operational requirements

The implementation can then be simplified into governed, reusable components.

## Interview Translation

I would begin by inventorying the Access tables, queries, macros, VBA
procedures, schedules, controls, and report outputs. I would then separate
source ingestion, transformation, exception handling, and reporting into
Bronze, Silver, and Gold layers. Business rules would move into tested SQL or
PySpark transformations, orchestration would move into Lakeflow Jobs, and
reporting-ready outputs would be prepared for Power BI.
