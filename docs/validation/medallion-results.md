# GLEIF Lakehouse Validation Results

## Execution Status

The Bronze, Silver, and Gold notebooks were executed successfully in
Databricks Free Edition using serverless compute.

## Bronze Results

| Metric | Result |
|---|---:|
| Total source rows | 6 |
| Distinct LEIs | 5 |
| Blank legal names | 1 |
| Non-active entities | 1 |

Bronze preserved the source records and their data-quality issues without
silently correcting or deleting them.

## Silver Results

| Dataset | Rows |
|---|---:|
| Trusted Silver records | 4 |
| Quarantined records | 1 |
| Duplicate audit records | 1 |

Silver standardized values and data types, quarantined the record with a
missing legal name, and retained the repeated LEI separately for auditability.

The sample treats repeated LEIs as duplicates for demonstration purposes.
In a production model, repeated LEIs may represent valid relationships between
multiple internal party records and one legal entity. The intended business
grain must therefore be established before deduplication rules are applied.

## Gold Results

Gold created three reporting-ready Delta tables:

- `gold_gleif_entity_reporting`
- `gold_gleif_status_summary`
- `gold_gleif_quality_metrics`

## Reconciliation

| Metric | Result |
|---|---:|
| Bronze rows | 6 |
| Silver rows | 4 |
| Quarantined rows | 1 |
| Duplicate audit rows | 1 |
| Reconciled output rows | 6 |

The output balances to the original Bronze input:

`4 trusted + 1 quarantined + 1 duplicate = 6 source rows`

## Architecture Translation

- Bronze = raw / staging
- Silver = standardized / intermediate
- Gold = business-ready / mart
