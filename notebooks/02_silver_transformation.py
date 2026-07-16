# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver GLEIF Transformation
# MAGIC
# MAGIC Transform the raw Bronze data into standardized,
# MAGIC deduplicated, and validated Silver datasets.
# MAGIC
# MAGIC **Silver responsibilities**
# MAGIC - Standardize values and data types
# MAGIC - Validate required fields
# MAGIC - Deduplicate entity records
# MAGIC - Quarantine invalid records
# MAGIC - Preserve rejected duplicates for auditability

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

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

print(f"Bronze source: {bronze_table}")

# COMMAND ----------

bronze_df = spark.table(bronze_table)

standardized_df = (
    bronze_df
    .withColumn(
        "lei",
        F.upper(F.trim(F.col("lei"))),
    )
    .withColumn(
        "legal_name",
        F.trim(F.col("legal_name")),
    )
    .withColumn(
        "legal_jurisdiction",
        F.upper(F.trim(F.col("legal_jurisdiction"))),
    )
    .withColumn(
        "entity_status",
        F.upper(F.trim(F.col("entity_status"))),
    )
    .withColumn(
        "registration_status",
        F.upper(F.trim(F.col("registration_status"))),
    )
    .withColumn(
        "last_update_date",
        F.to_date(
            F.col("last_update_date"),
            "yyyy-MM-dd",
        ),
    )
)

display(standardized_df)

# COMMAND ----------

validated_df = (
    standardized_df
    .withColumn(
        "_quality_issues",
        F.concat_ws(
            " | ",
            F.when(
                F.col("lei").isNull()
                | (F.col("lei") == ""),
                F.lit("MISSING_LEI"),
            ),
            F.when(
                F.length(F.col("lei")) != 20,
                F.lit("INVALID_LEI_LENGTH"),
            ),
            F.when(
                F.col("legal_name").isNull()
                | (F.col("legal_name") == ""),
                F.lit("MISSING_LEGAL_NAME"),
            ),
            F.when(
                F.col("last_update_date").isNull(),
                F.lit("INVALID_LAST_UPDATE_DATE"),
            ),
        ),
    )
    .withColumn(
        "_record_quality_status",
        F.when(
            F.col("_quality_issues") == "",
            F.lit("VALID"),
        ).otherwise(F.lit("QUARANTINED")),
    )
)

display(validated_df)

# COMMAND ----------

deduplication_window = (
    Window
    .partitionBy("lei")
    .orderBy(
        F.col("last_update_date").desc_nulls_last(),
        F.col("_ingested_at_utc").desc(),
    )
)

ranked_df = validated_df.withColumn(
    "_deduplication_rank",
    F.row_number().over(deduplication_window),
)

# COMMAND ----------

silver_df = (
    ranked_df
    .filter(
        (F.col("_record_quality_status") == "VALID")
        & (F.col("_deduplication_rank") == 1)
    )
)

quarantine_df = (
    ranked_df
    .filter(
        F.col("_record_quality_status") == "QUARANTINED"
    )
)

duplicate_df = (
    ranked_df
    .filter(
        (F.col("_record_quality_status") == "VALID")
        & (F.col("_deduplication_rank") > 1)
    )
)

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_table)
)

(
    duplicate_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(duplicate_table)
)

print(f"Silver table: {silver_table}")
print(f"Quarantine table: {quarantine_table}")
print(f"Duplicate audit table: {duplicate_table}")

# COMMAND ----------

validation_summary_df = (
    spark.createDataFrame(
        [
            ("bronze_rows", bronze_df.count()),
            ("silver_rows", silver_df.count()),
            ("quarantined_rows", quarantine_df.count()),
            ("duplicate_rows", duplicate_df.count()),
        ],
        ["validation_metric", "record_count"],
    )
)

display(validation_summary_df)
