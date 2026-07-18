# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_name = "workspace"
schema_name = "access_modernization"

bronze_party_table = (
    f"{catalog_name}.{schema_name}.bronze_party_master"
)
bronze_report_table = (
    f"{catalog_name}.{schema_name}.bronze_monthly_reporting"
)

silver_party_table = (
    f"{catalog_name}.{schema_name}.silver_party_master"
)
party_quarantine_table = (
    f"{catalog_name}.{schema_name}.silver_party_master_quarantine"
)

silver_report_table = (
    f"{catalog_name}.{schema_name}.silver_monthly_reporting"
)
report_quarantine_table = (
    f"{catalog_name}.{schema_name}.silver_monthly_reporting_quarantine"
)
report_duplicate_table = (
    f"{catalog_name}.{schema_name}.silver_monthly_reporting_duplicates"
)

print(f"Party source: {bronze_party_table}")
print(f"Reporting source: {bronze_report_table}")

# COMMAND ----------
party_id_window = Window.partitionBy("party_id")

party_standardized_df = (
    spark.table(bronze_party_table)
    .select(
        F.trim("party_id").alias("party_id"),
        F.trim("party_name").alias("party_name"),
        F.upper(F.trim("lei")).alias("lei"),
        F.upper(F.trim("country_code")).alias("country_code"),
        F.initcap(F.trim("risk_rating")).alias("risk_rating"),
        F.upper(F.trim("active_flag")).alias("active_flag"),
        F.to_date("onboarding_date").alias("onboarding_date"),
        F.to_timestamp(
            "source_updated_at",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ).alias("source_updated_at"),
        "_source_file",
        "_source_system",
        "_ingested_at_utc",
    )
    .withColumn(
        "_party_id_count",
        F.count(F.lit(1)).over(party_id_window),
    )
    .withColumn(
        "quality_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("party_id").isNull()
                | (F.col("party_id") == ""),
                F.lit("MISSING_PARTY_ID"),
            ),
            F.when(
                F.col("party_name").isNull()
                | (F.col("party_name") == ""),
                F.lit("MISSING_PARTY_NAME"),
            ),
            F.when(
                F.length("lei") != 20,
                F.lit("INVALID_LEI_LENGTH"),
            ),
            F.when(
                ~F.col("active_flag").isin("Y", "N"),
                F.lit("INVALID_ACTIVE_FLAG"),
            ),
            F.when(
                F.col("_party_id_count") > 1,
                F.lit("DUPLICATE_PARTY_ID"),
            ),
        ),
    )
)

silver_party_df = (
    party_standardized_df
    .filter(F.col("quality_reason") == "")
    .drop("_party_id_count", "quality_reason")
)

party_quarantine_df = (
    party_standardized_df
    .filter(F.col("quality_reason") != "")
    .drop("_party_id_count")
)

print(f"Trusted party rows: {silver_party_df.count()}")
print(f"Quarantined party rows: {party_quarantine_df.count()}")

# COMMAND ----------
display(silver_party_df)

# COMMAND ----------
display(party_quarantine_df)

# COMMAND ----------
report_business_key_window = Window.partitionBy(
    "report_month",
    "party_id",
)

report_latest_window = (
    Window.partitionBy("report_month", "party_id")
    .orderBy(F.col("source_updated_at").desc())
)

report_standardized_df = (
    spark.table(bronze_report_table)
    .select(
        F.to_date("report_month").alias("report_month"),
        F.trim("party_id").alias("party_id"),
        F.col("gross_exposure_usd")
        .cast("decimal(18,2)")
        .alias("gross_exposure_usd"),
        F.col("exception_count")
        .cast("integer")
        .alias("exception_count"),
        F.initcap(
            F.trim("submission_status")
        ).alias("submission_status"),
        F.trim("analyst_name").alias("analyst_name"),
        F.to_timestamp(
            "source_updated_at",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ).alias("source_updated_at"),
        "_source_file",
        "_source_system",
        "_ingested_at_utc",
    )
    .withColumn(
        "_business_key_count",
        F.count(F.lit(1)).over(report_business_key_window),
    )
    .withColumn(
        "_submission_rank",
        F.row_number().over(report_latest_window),
    )
)

report_duplicate_df = (
    report_standardized_df
    .filter(F.col("_submission_rank") > 1)
    .withColumn(
        "audit_reason",
        F.lit("SUPERSEDED_BY_LATER_SUBMISSION"),
    )
    .drop("_business_key_count")
)

report_latest_df = (
    report_standardized_df
    .filter(F.col("_submission_rank") == 1)
)

# COMMAND ----------
trusted_party_keys_df = (
    silver_party_df
    .select(
        F.col("party_id").alias("trusted_party_id")
    )
)

report_joined_df = (
    report_latest_df.alias("report")
    .join(
        trusted_party_keys_df.alias("party"),
        F.col("report.party_id")
        == F.col("party.trusted_party_id"),
        "left",
    )
    .select(
        "report.*",
        "party.trusted_party_id",
    )
    .withColumn(
        "quality_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("report_month").isNull(),
                F.lit("INVALID_REPORT_MONTH"),
            ),
            F.when(
                F.col("party_id").isNull()
                | (F.col("party_id") == ""),
                F.lit("MISSING_PARTY_ID"),
            ),
            F.when(
                F.col("gross_exposure_usd").isNull(),
                F.lit("INVALID_GROSS_EXPOSURE"),
            ),
            F.when(
                F.col("exception_count").isNull(),
                F.lit("INVALID_EXCEPTION_COUNT"),
            ),
            F.when(
                ~F.col("submission_status").isin(
                    "Completed",
                    "Pending",
                ),
                F.lit("INVALID_SUBMISSION_STATUS"),
            ),
            F.when(
                F.col("trusted_party_id").isNull(),
                F.lit("PARTY_NOT_IN_TRUSTED_MASTER"),
            ),
        ),
    )
)

silver_report_df = (
    report_joined_df
    .filter(F.col("quality_reason") == "")
    .drop(
        "_business_key_count",
        "_submission_rank",
        "trusted_party_id",
        "quality_reason",
    )
)

report_quarantine_df = (
    report_joined_df
    .filter(F.col("quality_reason") != "")
    .drop(
        "_business_key_count",
        "_submission_rank",
        "trusted_party_id",
    )
)

print(f"Trusted reporting rows: {silver_report_df.count()}")
print(f"Quarantined reporting rows: {report_quarantine_df.count()}")
print(f"Duplicate audit rows: {report_duplicate_df.count()}")

# COMMAND ----------
display(silver_report_df)

# COMMAND ----------
display(report_quarantine_df)

# COMMAND ----------
display(report_duplicate_df)

# COMMAND ----------
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{catalog_name}.{schema_name}"
)

(
    silver_party_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_party_table)
)

(
    party_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(party_quarantine_table)
)

(
    silver_report_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_report_table)
)

(
    report_quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(report_quarantine_table)
)

(
    report_duplicate_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(report_duplicate_table)
)

print(f"Created: {silver_party_table}")
print(f"Created: {party_quarantine_table}")
print(f"Created: {silver_report_table}")
print(f"Created: {report_quarantine_table}")
print(f"Created: {report_duplicate_table}")

# COMMAND ----------
silver_reconciliation_df = spark.sql(
    f"""
    SELECT
        'party_master' AS source_dataset,
        (SELECT COUNT(*) FROM {bronze_party_table})
            AS bronze_rows,
        (SELECT COUNT(*) FROM {silver_party_table})
            AS trusted_rows,
        (SELECT COUNT(*) FROM {party_quarantine_table})
            AS quarantined_rows,
        0 AS duplicate_rows,
        (
            (SELECT COUNT(*) FROM {silver_party_table})
            +
            (SELECT COUNT(*) FROM {party_quarantine_table})
        ) AS reconciled_rows

    UNION ALL

    SELECT
        'monthly_reporting' AS source_dataset,
        (SELECT COUNT(*) FROM {bronze_report_table})
            AS bronze_rows,
        (SELECT COUNT(*) FROM {silver_report_table})
            AS trusted_rows,
        (SELECT COUNT(*) FROM {report_quarantine_table})
            AS quarantined_rows,
        (SELECT COUNT(*) FROM {report_duplicate_table})
            AS duplicate_rows,
        (
            (SELECT COUNT(*) FROM {silver_report_table})
            +
            (SELECT COUNT(*) FROM {report_quarantine_table})
            +
            (SELECT COUNT(*) FROM {report_duplicate_table})
        ) AS reconciled_rows
    """
)

display(silver_reconciliation_df)
