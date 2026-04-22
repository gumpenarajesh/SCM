# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8eb1159-afd1-49f6-afb6-7d3267c1ab39",
# META       "default_lakehouse_name": "SCM_Bronze_LH",
# META       "default_lakehouse_workspace_id": "f6eefe30-09e0-42fd-b87d-b4b372c2de7a",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8eb1159-afd1-49f6-afb6-7d3267c1ab39"
# META         },
# META         {
# META           "id": "ffa69779-942a-4e38-86f2-c60e9c99e21c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, regexp_replace, when

# Load base tables
kna1 = spark.table("SAP.KNA1").filter("MANDT = '100'")
knvv = spark.table("SAP.KNVV").filter("MANDT = '100'")
knvp = spark.table("SAP.KNVP").filter("MANDT = '100'")
knvh = spark.table("SAP.KNVH").filter("MANDT = '100'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.customer_audit LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SOLD TO
# -------------------------
# filter(col("PARVW") == "AG")
sold_to = knvp \
.select(
    regexp_replace(col("KUNN2"), "^0+", "").alias("KUNN2_ST"),
    col("KUNNR").alias("KUNNR_ST"),
    col("VKORG").alias("VKORG_ST"),
    col("SPART").alias("SPART_ST"),
    col("VTWEG").alias("VTWEG_ST")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SHIP TO
# -------------------------
# .filter(col("PARVW") == "WE")
ship_to = knvp \
.select(
    regexp_replace(col("KUNN2"), "^0+", "").alias("KUNN2_SHIPT"),
    col("KUNNR").alias("KUNNR_SHIPT"),
    col("VKORG").alias("VKORG_SHIPT"),
    col("SPART").alias("SPART_SHIPT"),
    col("VTWEG").alias("VTWEG_SHIPT")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BILL TO
# -------------------------
# .filter(col("PARVW") == "RE")
bill_to = knvp \
.select(
    regexp_replace(col("KUNN2"), "^0+", "").alias("KUNN2_BT"),
    col("KUNNR").alias("KUNNR_BT"),
    col("VKORG").alias("VKORG_BT"),
    col("SPART").alias("SPART_BT"),
    col("VTWEG").alias("VTWEG_BT")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PAYER
# -------------------------
# .filter(col("PARVW") == "RG")
payer = knvp \
.select(
    regexp_replace(col("KUNN2"), "^0+", "").alias("KUNN2_PAY"),
    col("KUNNR").alias("KUNNR_PAY"),
    col("VKORG").alias("VKORG_PAY"),
    col("SPART").alias("SPART_PAY"),
    col("VTWEG").alias("VTWEG_PAY")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CUSTOMER HIERARCHIES
# -------------------------

customer_hierarchies = knvh.select(
    regexp_replace(col("HKUNNR"), "^0+", "").alias("HGLVCUST_1"),
    col("KUNNR").alias("KUNNR_H"),
    col("VKORG").alias("VKORG_H"),
    col("VTWEG").alias("VTWEG_H"),
    col("SPART").alias("SPART_H")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pal1 = knvh.select(
    col("HKUNNR").alias("STANDARD_HGLVCUST"),
    col("KUNNR").alias("KUNNR_HZ"),
    col("VKORG").alias("VKORG_HZ"),
    col("VTWEG").alias("VTWEG_HZ"),
    col("SPART").alias("SPART_HZ")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = kna1.join(
    knvv,
    col("kna1.KUNNR") == col("knvv.KUNNR"),
    "left"
)

result = result.join(
    sold_to,
    (col("knvv.KUNNR") == col("KUNNR_ST")) &
    (col("knvv.VKORG") == col("VKORG_ST")) &
    (col("knvv.VTWEG") == col("VTWEG_ST")) &
    (col("knvv.SPART") == col("SPART_ST")),
    "left"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = result.join(
    ship_to,
    (col("knvv.KUNNR") == col("KUNNR_SHIPT")) &
    (col("knvv.VKORG") == col("VKORG_SHIPT")) &
    (col("knvv.VTWEG") == col("VTWEG_SHIPT")) &
    (col("knvv.SPART") == col("SPART_SHIPT")),
    "left"
)

result = result.join(
    bill_to,
    (col("knvv.KUNNR") == col("KUNNR_BT")) &
    (col("knvv.VKORG") == col("VKORG_BT")) &
    (col("knvv.VTWEG") == col("VTWEG_BT")) &
    (col("knvv.SPART") == col("SPART_BT")),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = result.join(
    payer,
    (col("knvv.KUNNR") == col("KUNNR_PAY")) &
    (col("knvv.VKORG") == col("VKORG_PAY")) &
    (col("knvv.VTWEG") == col("VTWEG_PAY")) &
    (col("knvv.SPART") == col("SPART_PAY")),
    "left"
)

result = result.join(
    customer_hierarchies,
    (col("knvv.KUNNR") == col("KUNNR_H")) &
    (col("knvv.VKORG") == col("VKORG_H")) &
    (col("knvv.VTWEG") == col("VTWEG_H")) &
    (col("knvv.SPART") == col("SPART_H")),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = result.join(
    pal1,
    (col("knvv.KUNNR") == col("KUNNR_HZ")) &
    (col("knvv.VKORG") == col("VKORG_HZ")) &
    (col("knvv.VTWEG") == col("VTWEG_HZ")) &
    (col("knvv.SPART") == col("SPART_HZ")),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


final_df = result.select(

    regexp_replace(col("kna1.KUNNR"), "^0+", "").alias("Customer"),

    col("KUNN2_ST").alias("Sold To"),

    when(col("KUNN2_BT") != "", col("KUNN2_BT"))
    .otherwise(col("KUNN2_SHIPT"))
    .alias("Bill To"),

    col("KUNN2_SHIPT").alias("Ship To"),

    when(col("KUNN2_PAY") != "", col("KUNN2_PAY"))
    .otherwise(col("KUNN2_ST"))
    .alias("Payer"),

    col("HGLVCUST_1").alias("HglvCust"),

    col("kna1.NAME1").alias("Name"),
    col("kna1.LAND1").alias("Country"),

    col("STANDARD_HGLVCUST").alias("Pal1 Customer"),

    col("knvv.VKORG").alias("Sales Org")
)

final_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in final_df.columns:
    clean_name = c.strip().replace(" ", "_")
    final_df = final_df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

original_unique_count = final_df.select("Customer").distinct().count()

print(f"Unique IDs in Original: {original_unique_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df1=final_df.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 final_df1_clean = final_df1.dropna(subset=["Sold_To","Bill_To","Ship_To"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df1_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


 
final_df1_clean.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.analytics.customer_audit")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df1.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df1_clean.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.customer_audit LIMIT 1000")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

original_unique_count = final_df.select("Customer").distinct().count()
dropped_unique_count = final_df1.select("Customer").distinct().count()

print(f"Unique IDs in Original: {original_unique_count}")
print(f"Unique IDs in Dropped : {dropped_unique_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(knvp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
