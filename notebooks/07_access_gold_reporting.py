# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

catalog_name = "workspace"
schema_name = "access_modernization"

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

bronze_party_table = (
    f"{catalog_name}.{schema_name}.bronze_party_master"
)
bronze_report_table = (
    f"{catalog_name}.{schema_name}.bronze_monthly_reporting"
)

gold_reporting_table = (
    f"{catalog_name}.{schema_name}.gold_party_reporting"
)
gold_analyst_table = (
    f"{catalog_name}.{schema_name}.gold_analyst_summary"
)
gold_portfolio_table = (
    f"{catalog_name}.{schema_name}.gold_portfolio_summary"
)
gold_quality_table = (
    f"{catalog_name}.{schema_name}.gold_quality_metrics"
)

print(f"Party source: {silver_party_table}")
print(f"Reporting source: {silver_report_table}")

# COMMAND ----------
party_df = spark.table(silver_party_table)
report_df = spark.table(silver_report_table)

gold_party_reporting_df = (
    report_df.alias("report")
    .join(
        party_df.alias("party"),
        on="party_id",
        how="inner",
    )
    .select(
        F.col("report.report_month"),
        F.col("party_id"),
        F.col("party.party_name"),
        F.col("party.lei"),
        F.col("party.country_code"),
        F.col("party.risk_rating"),
        F.col("party.active_flag"),
        F.col("party.onboarding_date"),
        F.col("report.gross_exposure_usd"),
        F.col("report.exception_count"),
        F.col("report.submission_status"),
        F.col("report.analyst_name"),
        F.col("report.source_updated_at"),
    )
    .withColumn(
        "active_status",
        F.when(
            F.col("active_flag") == "Y",
            F.lit("Active"),
        ).otherwise(F.lit("Inactive")),
    )
    .withColumn(
        "requires_exception_review",
        F.when(
            F.col("exception_count") > 0,
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "review_priority",
        F.when(
            (F.col("risk_rating") == "High")
            & (F.col("exception_count") > 0),
            F.lit("High"),
        )
        .when(
            (F.col("active_flag") == "N")
            | (F.col("exception_count") > 0),
            F.lit("Medium"),
        )
        .otherwise(F.lit("Standard")),
    )
    .withColumn(
        "reporting_ready",
        F.when(
            (F.col("submission_status") == "Completed")
            & (F.col("active_flag") == "Y"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "_gold_created_at_utc",
        F.current_timestamp(),
    )
)

print(
    f"Gold party-reporting rows: "
    f"{gold_party_reporting_df.count()}"
)

# COMMAND ----------
display(gold_party_reporting_df)

# COMMAND ----------
zero_decimal = F.lit(0).cast("decimal(18,2)")

gold_analyst_summary_df = (
    gold_party_reporting_df
    .groupBy(
        "report_month",
        "analyst_name",
    )
    .agg(
        F.countDistinct("party_id").alias("party_count"),
        F.sum("gross_exposure_usd").alias(
            "total_gross_exposure_usd"
        ),
        F.sum("exception_count").alias(
            "total_exception_count"
        ),
        F.sum(
            F.when(
                F.col("risk_rating") == "High",
                F.col("gross_exposure_usd"),
            ).otherwise(zero_decimal)
        ).alias("high_risk_exposure_usd"),
        F.sum(
            F.when(
                F.col("requires_exception_review"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("review_required_count"),
    )
    .withColumn(
        "average_exposure_usd",
        F.round(
            F.col("total_gross_exposure_usd")
            / F.col("party_count"),
            2,
        ),
    )
    .withColumn(
        "_gold_created_at_utc",
        F.current_timestamp(),
    )
    .orderBy(
        "report_month",
        F.col("total_gross_exposure_usd").desc(),
    )
)

display(gold_analyst_summary_df)

# COMMAND ----------
gold_portfolio_summary_df = (
    gold_party_reporting_df
    .groupBy("report_month")
    .agg(
        F.countDistinct("party_id").alias("party_count"),
        F.sum("gross_exposure_usd").alias(
            "total_gross_exposure_usd"
        ),
        F.sum("exception_count").alias(
            "total_exception_count"
        ),
        F.sum(
            F.when(
                F.col("risk_rating") == "High",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("high_risk_party_count"),
        F.sum(
            F.when(
                F.col("active_flag") == "N",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("inactive_party_count"),
        F.sum(
            F.when(
                F.col("review_priority").isin(
                    "High",
                    "Medium",
                ),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("review_required_count"),
        F.sum(
            F.when(
                F.col("reporting_ready"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("reporting_ready_count"),
    )
    .withColumn(
        "_gold_created_at_utc",
        F.current_timestamp(),
    )
)

display(gold_portfolio_summary_df)

# COMMAND ----------
quality_metrics = [
    (
        "bronze_party_rows",
        spark.table(bronze_party_table).count(),
    ),
    (
        "silver_party_trusted_rows",
        spark.table(silver_party_table).count(),
    ),
    (
        "silver_party_quarantine_rows",
        spark.table(party_quarantine_table).count(),
    ),
    (
        "bronze_reporting_rows",
        spark.table(bronze_report_table).count(),
    ),
    (
        "silver_reporting_trusted_rows",
        spark.table(silver_report_table).count(),
    ),
    (
        "silver_reporting_quarantine_rows",
        spark.table(report_quarantine_table).count(),
    ),
    (
        "silver_reporting_duplicate_rows",
        spark.table(report_duplicate_table).count(),
    ),
    (
        "gold_reporting_rows",
        gold_party_reporting_df.count(),
    ),
]

gold_quality_metrics_df = (
    spark.createDataFrame(
        quality_metrics,
        ["metric_name", "metric_value"],
    )
    .withColumn(
        "_gold_created_at_utc",
        F.current_timestamp(),
    )
)

display(gold_quality_metrics_df)

# COMMAND ----------
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{catalog_name}.{schema_name}"
)

(
    gold_party_reporting_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_reporting_table)
)

(
    gold_analyst_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_analyst_table)
)

(
    gold_portfolio_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_portfolio_table)
)

(
    gold_quality_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_quality_table)
)

print(f"Created: {gold_reporting_table}")
print(f"Created: {gold_analyst_table}")
print(f"Created: {gold_portfolio_table}")
print(f"Created: {gold_quality_table}")

# COMMAND ----------
gold_validation_df = spark.sql(
    f"""
    SELECT
        'gold_party_reporting' AS gold_dataset,
        COUNT(*) AS row_count
    FROM {gold_reporting_table}

    UNION ALL

    SELECT
        'gold_analyst_summary' AS gold_dataset,
        COUNT(*) AS row_count
    FROM {gold_analyst_table}

    UNION ALL

    SELECT
        'gold_portfolio_summary' AS gold_dataset,
        COUNT(*) AS row_count
    FROM {gold_portfolio_table}

    UNION ALL

    SELECT
        'gold_quality_metrics' AS gold_dataset,
        COUNT(*) AS row_count
    FROM {gold_quality_table}
    """
)

display(gold_validation_df)
