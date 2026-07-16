# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze GLEIF Ingestion
# MAGIC
# MAGIC Load the controlled GLEIF-style CSV without correcting its contents.
# MAGIC
# MAGIC **Bronze responsibilities**
# MAGIC - Preserve source values
# MAGIC - Record ingestion metadata
# MAGIC - Establish baseline counts
# MAGIC - Write a Delta table for downstream transformation

# COMMAND ----------

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F


def find_repo_root(start_path: Path) -> Path:
    """Find the repository root containing the controlled sample file."""
    candidates = [start_path, *start_path.parents]

    for candidate in candidates:
        sample_file = (
            candidate
            / "data"
            / "sample"
            / "gleif_entities_sample.csv"
        )

        if sample_file.exists():
            return candidate

    raise FileNotFoundError(
        "Could not locate data/sample/gleif_entities_sample.csv "
        f"starting from {start_path}"
    )


repo_root = find_repo_root(Path.cwd())
source_path = (
    repo_root
    / "data"
    / "sample"
    / "gleif_entities_sample.csv"
)

print(f"Repository root: {repo_root}")
print(f"Source file: {source_path}")

# COMMAND ----------

# Preserve blank strings and all source columns exactly as supplied.
source_pdf = pd.read_csv(
    source_path,
    dtype=str,
    keep_default_na=False,
)

bronze_df = (
    spark.createDataFrame(source_pdf)
    .withColumn(
        "_source_file",
        F.lit(source_path.name),
    )
    .withColumn(
        "_source_system",
        F.lit("controlled_gleif_sample"),
    )
    .withColumn(
        "_ingested_at_utc",
        F.current_timestamp(),
    )
)

display(bronze_df)

# COMMAND ----------

print(f"Bronze record count: {bronze_df.count()}")
bronze_df.printSchema()

# COMMAND ----------

current_catalog = spark.sql(
    "SELECT current_catalog() AS catalog_name"
).first()["catalog_name"]

target_schema = "gleif_lakehouse"
bronze_table = f"{current_catalog}.{target_schema}.bronze_gleif_entities"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"{current_catalog}.{target_schema}"
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze Delta table created: {bronze_table}")

# COMMAND ----------

bronze_validation_df = spark.sql(
    f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT lei) AS distinct_lei_count,
        SUM(
            CASE
                WHEN TRIM(legal_name) = '' THEN 1
                ELSE 0
            END
        ) AS blank_legal_name_count,
        SUM(
            CASE
                WHEN entity_status <> 'ACTIVE' THEN 1
                ELSE 0
            END
        ) AS non_active_entity_count
    FROM {bronze_table}
    """
)

display(bronze_validation_df)
