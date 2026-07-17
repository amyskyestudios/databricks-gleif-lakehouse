# Databricks notebook source
# COMMAND ----------
from functools import reduce
from pyspark.sql import functions as F

catalog_name = "workspace"
schema_name = "gleif_lakehouse"

tables = {
    "bronze_entities": f"{catalog_name}.{schema_name}.bronze_gleif_entities",
    "silver_entities": f"{catalog_name}.{schema_name}.silver_gleif_entities",
    "silver_quarantine": f"{catalog_name}.{schema_name}.silver_gleif_entities_quarantine",
    "silver_duplicates": f"{catalog_name}.{schema_name}.silver_gleif_entities_duplicates",
    "gold_entity_reporting": f"{catalog_name}.{schema_name}.gold_gleif_entity_reporting",
    "gold_status_summary": f"{catalog_name}.{schema_name}.gold_gleif_status_summary",
    "gold_quality_metrics": f"{catalog_name}.{schema_name}.gold_gleif_quality_metrics",
}

print(f"Auditing {len(tables)} Delta tables")

# COMMAND ----------
history_frames = []

for table_role, table_name in tables.items():
    history_df = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .select(
            F.lit(table_role).alias("table_role"),
            F.lit(table_name).alias("table_name"),
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "operationMetrics",
        )
    )
    history_frames.append(history_df)

combined_history = reduce(
    lambda left, right: left.unionByName(right),
    history_frames,
)

display(
    combined_history.orderBy(
        F.col("table_role"),
        F.col("version").desc(),
    )
)

# COMMAND ----------
detail_frames = []

for table_role, table_name in tables.items():
    detail_df = (
        spark.sql(f"DESCRIBE DETAIL {table_name}")
        .select(
            F.lit(table_role).alias("table_role"),
            F.lit(table_name).alias("table_name"),
            "format",
            "numFiles",
            "sizeInBytes",
            "createdAt",
            "lastModified",
        )
    )
    detail_frames.append(detail_df)

combined_details = reduce(
    lambda left, right: left.unionByName(right),
    detail_frames,
)

display(combined_details.orderBy("table_role"))
