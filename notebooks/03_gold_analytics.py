# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold GLEIF Analytics
# MAGIC
# MAGIC Produce business-ready entity data, status summaries,
# MAGIC and pipeline quality metrics from the trusted Silver layer.
# MAGIC
# MAGIC **Gold responsibilities**
# MAGIC - Deliver reporting-ready datasets
# MAGIC - Add useful business classifications
# MAGIC - Aggregate operational metrics
# MAGIC - Reconcile Bronze, Silver, quarantine, and duplicate counts

# COMMAND ----------

current_catalog = spark.sql(
    "SELECT current_catalog() AS catalog_name"
).first()["catalog_name"]

target_schema = "gleif_lakehouse"

bronze_table = (
    f"{current_catalog}.{target_schema}.bronze_gleif_entities"
)

silver_table = (
    f"{current_catalog}.{target_schema}.silver_gleif_entities"
)

quarantine_table = (
    f"{current_catalog}.{target_schema}."
    "silver_gleif_entities_quarantine"
)

duplicate_table = (
    f"{current_catalog}.{target_schema}."
    "silver_gleif_entities_duplicates"
)

gold_entity_table = (
    f"{current_catalog}.{target_schema}.gold_gleif_entity_reporting"
)

gold_status_table = (
    f"{current_catalog}.{target_schema}.gold_gleif_status_summary"
)

gold_quality_table = (
    f"{current_catalog}.{target_schema}.gold_gleif_quality_metrics"
)

print(f"Silver source: {silver_table}")

# COMMAND ----------

# [Spark SQL + Delta Lake]
# Create a business-facing entity-level reporting table.

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {gold_entity_table}
    USING DELTA
    AS
    SELECT
        lei,
        legal_name,
        legal_jurisdiction,
        entity_status,
        registration_status,
        last_update_date,
        DATEDIFF(
            CURRENT_DATE(),
            last_update_date
        ) AS days_since_last_update,
        CASE
            WHEN entity_status = 'ACTIVE'
                 AND registration_status = 'ISSUED'
                THEN 'CURRENT'
            WHEN entity_status = 'INACTIVE'
                THEN 'INACTIVE_ENTITY'
            ELSE 'REVIEW'
        END AS reporting_status
    FROM {silver_table}
    """
)

print(f"Gold entity table created: {gold_entity_table}")

# COMMAND ----------

# [Spark SQL + Delta Lake]
# Create summarized metrics suitable for dashboards or Power BI.

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {gold_status_table}
    USING DELTA
    AS
    SELECT
        legal_jurisdiction,
        entity_status,
        registration_status,
        reporting_status,
        COUNT(*) AS entity_count,
        MAX(last_update_date) AS most_recent_update_date
    FROM {gold_entity_table}
    GROUP BY
        legal_jurisdiction,
        entity_status,
        registration_status,
        reporting_status
    """
)

print(f"Gold status summary created: {gold_status_table}")

# COMMAND ----------

# [Spark SQL + Delta Lake]
# Publish pipeline quality and reconciliation metrics.

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {gold_quality_table}
    USING DELTA
    AS
    SELECT
        'bronze_rows' AS metric_name,
        COUNT(*) AS metric_value
    FROM {bronze_table}

    UNION ALL

    SELECT
        'silver_rows',
        COUNT(*)
    FROM {silver_table}

    UNION ALL

    SELECT
        'quarantined_rows',
        COUNT(*)
    FROM {quarantine_table}

    UNION ALL

    SELECT
        'duplicate_rows',
        COUNT(*)
    FROM {duplicate_table}

    UNION ALL

    SELECT
        'reconciled_output_rows',
        (
            (SELECT COUNT(*) FROM {silver_table})
            + (SELECT COUNT(*) FROM {quarantine_table})
            + (SELECT COUNT(*) FROM {duplicate_table})
        )
    """
)

print(f"Gold quality metrics created: {gold_quality_table}")

# COMMAND ----------

# [Databricks display + Spark SQL]

display(
    spark.sql(
        f"""
        SELECT *
        FROM {gold_entity_table}
        ORDER BY legal_name
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {gold_status_table}
        ORDER BY
            legal_jurisdiction,
            reporting_status
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {gold_quality_table}
        ORDER BY metric_name
        """
    )
)
