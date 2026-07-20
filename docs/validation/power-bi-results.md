# Power BI Access Modernization Dashboard Validation

## Purpose

This dashboard demonstrates how business-ready Databricks Gold tables can support a Power BI reporting and validation layer for a representative Microsoft Access modernization workflow.

The report is intentionally focused on traceability, business-rule validation, and operational reporting rather than decorative dashboard design.

## Connection and Refresh

Power BI Desktop connects directly to the Databricks SQL warehouse using OAuth (OIDC).

The report uses Import mode, which stores a local model copy while preserving the Databricks connection for future refreshes.

A manual Power BI refresh successfully:

- reconnected to Databricks
- started the stopped SQL warehouse
- evaluated all four Gold queries
- reloaded the imported model
- preserved the validated dashboard totals without errors

This confirms that the report is backed by refreshable Databricks Gold outputs rather than a static exported file.

## Gold Tables Used

### gold_party_reporting

Business-ready party-level reporting table.

Each row represents one trusted party and includes:

- party identity
- assigned analyst
- gross exposure
- exception count
- risk rating
- exception-review requirement
- review priority
- reporting-readiness status

This table supports the KPI measures and the traceable party-detail view.

### gold_analyst_summary

Aggregated reporting table at the analyst level.

It supports the Gross Exposure by Analyst chart and provides an operational view of analyst workload and portfolio exposure.

### gold_portfolio_summary

Single-row executive portfolio summary containing validated portfolio-wide totals.

It provides a Databricks-side comparison point for the Power BI KPI results.

### gold_quality_metrics

Named validation controls produced by the Gold pipeline.

The Power BI quality table exposes all eight controls so report users can review pipeline counts and reconciliation evidence alongside the business outputs.

## Model Design Decision

Power BI automatically proposed a relationship between:

- gold_analyst_summary[analyst_name]
- gold_party_reporting[analyst_name]

That relationship was removed intentionally.

gold_analyst_summary is an aggregated reporting output, not a true analyst dimension. Connecting it directly to the party-detail table could create ambiguous filtering or double-counting.

For this validation-focused prototype, the Gold outputs remain disconnected so each visual operates at its intended reporting grain. A broader production semantic model could introduce conformed analyst, party, and date dimensions when coordinated cross-filtering is required.

## DAX Measures

### Total Gross Exposure

Sums gross exposure across trusted parties.

Validated result: **$4,610,000**

### Total Exceptions

Sums the exception count across trusted parties.

Validated result: **3**

### Party Count

Distinct count of trusted party IDs.

Validated result: **5**

### High-Risk Exposure

Calculates gross exposure only for parties with a High risk rating.

Validated result: **$990,000**

### Review-Required Count

Counts trusted parties whose exception-review flag is true.

Validated result: **3**

### Reporting-Ready Percentage

Divides reporting-ready trusted parties by the total trusted-party count.

Validated result: **80%**

### Analyst Gross Exposure

Explicitly sums analyst-level gross exposure from gold_analyst_summary.

This measure replaces Power BI's implicit Sum of aggregation with a named, governed business measure.

## Dashboard Areas

### Executive KPI Cards

The six KPI cards summarize the trusted Gold reporting population:

- total portfolio exposure
- total exceptions
- trusted party count
- high-risk exposure
- parties requiring review
- percentage ready for reporting

These are not manually entered values. Each card is driven by a defined DAX business rule.

### Gross Exposure by Analyst

The horizontal bar chart compares analyst-level portfolio exposure.

Validated analyst totals:

- Jordan Lee: **$2,750,000**
- Avery Chen: **$1,110,000**
- Sam Patel: **$750,000**

The chart uses the analyst-summary Gold table at its intended aggregate grain.

### Party-Level Validation Detail

The detail table displays the five trusted party records underlying the KPIs.

It provides record-level traceability for:

- party identity
- analyst ownership
- gross exposure
- exceptions
- risk
- review priority
- reporting readiness

The visible row values reconcile to the KPI totals.

### Gold Quality Metrics

The quality table displays the eight named Gold validation controls without aggregating their values.

This keeps pipeline-quality evidence visible beside the reporting results and supports troubleshooting and audit conversations.

## Reconciliation Results

Power BI results match the validated Databricks Gold outputs:

- trusted parties: **5**
- total gross exposure: **$4,610,000**
- total exceptions: **3**
- high-risk exposure: **$990,000**
- review-required parties: **3**
- reporting-ready parties: **4 of 5**
- reporting-ready percentage: **80%**

## Final Dashboard

![Access modernization Power BI dashboard](../images/power-bi/access-modernization-dashboard-final.png)

## Artifact

The working Power BI Desktop report is stored at:

[Download the Power BI Desktop report](../../power-bi/access-modernization-dashboard.pbix)

## Modernization Significance

This prototype demonstrates a controlled transition from legacy Access-style reporting patterns to:

- Databricks for ingestion, transformation, validation, and governed Gold outputs
- Lakeflow Jobs for orchestration
- Power BI for refreshable semantic measures, operational reporting, and traceable presentation

A direct Access-to-Power-BI connection can be useful for baseline profiling and parallel validation, but it does not migrate Access forms, VBA, macros, writeback behavior, or scheduling logic.

The preferred modernization pattern is:

Access baseline → Databricks replacement → parallel Power BI validation → Gold-table cutover

## What This Demonstrates

This representative prototype extends the Databricks Access-modernization workflow into a refreshable Power BI reporting layer. It validates Gold-layer business rules through explicit DAX measures, preserves each reporting table at its intended grain, removes an inappropriate auto-generated relationship, reconciles dashboard results to the Databricks Gold totals, and confirms that Power BI can refresh through the Databricks SQL warehouse using OAuth.