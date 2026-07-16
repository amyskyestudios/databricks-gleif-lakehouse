# Databricks notebook source
print("Hello from Databricks!")
spark.version
gleif_data = [
    ("5493001KJTIIGC8Y1R12", "BLUE SKY HOLDINGS LLC", "ACTIVE", "ISSUED", None),
    ("213800D1EI4B9WTWWD28", "BLUE SKY HOLDING LIMITED", "ACTIVE", "ISSUED", None),
    ("984500ABC123EXAMPLE1", "SUNRISE TRADING INC", "INACTIVE", "LAPSED", None),
    ("549300OLDMERGED0001", "OLD MERGED ENTITY LTD", "INACTIVE", "RETIRED", "5493001KJTIIGC8Y1R12"),
]

gleif_cols = ["lei", "legal_name", "entity_status", "registration_status", "successor_lei"]

gleif_df = spark.createDataFrame(gleif_data, gleif_cols)

display(gleif_df)

party_data = [
    (1001, "Blue Sky Holdings LLC", None, "PRINCIPAL"),
    (1002, "Blue Sky Holding Ltd", None, "AGENT"),
    (1003, "Sunrise Trading Incorporated", "984500ABC123EXAMPLE1", "PRINCIPAL"),
    (1004, "Unknown Moonlight Corp", None, "INDIRECT_PRINCIPAL"),
]

party_cols = ["party_id", "party_name", "existing_lei", "party_type"]

party_df = spark.createDataFrame(party_data, party_cols)

display(party_df)

from pyspark.sql.functions import upper, trim

party_clean = party_df.withColumn("party_name_clean", upper(trim("party_name")))
gleif_clean = gleif_df.withColumn("legal_name_clean", upper(trim("legal_name")))

exact_name_matches = party_clean.join(
    gleif_clean,
    party_clean.party_name_clean == gleif_clean.legal_name_clean,
    "left"
)

display(exact_name_matches)

from pyspark.sql.functions import col

unmatched = exact_name_matches.filter(
    col("lei").isNull()
)

display(unmatched)

from pyspark.sql.functions import regexp_replace, upper, trim

def normalize_name(col_name):
    cleaned = upper(trim(col(col_name)))
    cleaned = regexp_replace(cleaned, r"\bLTD\b", "LIMITED")
    cleaned = regexp_replace(cleaned, r"\bINC\b", "INCORPORATED")
    cleaned = regexp_replace(cleaned, r"[^A-Z0-9 ]", "")
    cleaned = regexp_replace(cleaned, r"\s+", " ")
    return trim(cleaned)

party_norm = party_df.withColumn("party_name_norm", normalize_name("party_name"))
gleif_norm = gleif_df.withColumn("legal_name_norm", normalize_name("legal_name"))

display(party_norm)
display(gleif_norm)

candidate_matches = party_norm.join(
    gleif_norm,
    party_norm.party_name_norm == gleif_norm.legal_name_norm,
    "left"
)

display(candidate_matches)

from pyspark.sql.functions import when

match_results = candidate_matches.withColumn(
    "match_status",
    when(col("lei").isNotNull(), "NAME_MATCH")
    .otherwise("NO_MATCH")
)

display(match_results.select(
    "party_id",
    "party_name",
    "existing_lei",
    "party_type",
    "lei",
    "legal_name",
    "entity_status",
    "registration_status",
    "successor_lei",
    "match_status"
))