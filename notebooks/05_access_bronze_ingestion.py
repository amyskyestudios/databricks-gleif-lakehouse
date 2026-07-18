# Databricks notebook source
# COMMAND ----------
from pathlib import Path
import pandas as pd
from pyspark.sql import functions as F

source_folder_name = "access_modernization"

repo_root = next(
    (
        path
        for path in [Path.cwd(), *Path.cwd().parents]
        if (path / "data" / "sample" / source_folder_name).exists()
    ),
    None,
)

if repo_root is None:
    raise FileNotFoundError(
        "Could not locate data/sample/access_modernization from the notebook path."
    )

source_dir = repo_root / "data" / "sample" / source_folder_name
party_source_path = source_dir / "party_master_export.csv"
report_source_path = source_dir / "monthly_reporting_export.csv"

party_pd = pd.read_csv(
    party_source_path,
    dtype=str,
    keep_default_na=False,
)

report_pd = pd.read_csv(
    report_source_path,
    dtype=str,
    keep_default_na=False,
)

print(f"Party source rows: {len(party_pd)}")
print(f"Monthly reporting source rows: {len(report_pd)}")
print(f"Source folder: {source_dir}")

# COMMAND ----------
party_bronze_df = (
    spark.createDataFrame(party_pd)
    .withColumn("_source_file", F.lit(party_source_path.name))
    .withColumn("_source_system", F.lit("Microsoft Access export"))
    .withColumn("_ingested_at_utc", F.current_timestamp())
)

report_bronze_df = (
    spark.createDataFrame(report_pd)
    .withColumn("_source_file", F.lit(report_source_path.name))
    .withColumn("_source_system", F.lit("Microsoft Access export"))
    .withColumn("_ingested_at_utc", F.current_timestamp())
)

print(f"Party Bronze rows: {party_bronze_df.count()}")
print(f"Reporting Bronze rows: {report_bronze_df.count()}")

# COMMAND ----------
display(party_bronze_df)

# COMMAND ----------
display(report_bronze_df)

# COMMAND ----------
catalog_name = "workspace"
schema_name = "access_modernization"

party_table = f"{catalog_name}.{schema_name}.bronze_party_master"
report_table = f"{catalog_name}.{schema_name}.bronze_monthly_reporting"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

(
    party_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(party_table)
)

(
    report_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(report_table)
)

print(f"Created: {party_table}")
print(f"Created: {report_table}")

# COMMAND ----------
bronze_validation = spark.sql(
    f"""
    SELECT
        'party_master' AS source_dataset,
        COUNT(*) AS row_count,
        COUNT(DISTINCT party_id) AS distinct_business_keys
    FROM {party_table}

    UNION ALL

    SELECT
        'monthly_reporting' AS source_dataset,
        COUNT(*) AS row_count,
        COUNT(DISTINCT CONCAT(report_month, '|', party_id))
            AS distinct_business_keys
    FROM {report_table}
    """
)

display(bronze_validation)
