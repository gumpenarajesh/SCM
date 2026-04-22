# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffa69779-942a-4e38-86f2-c60e9c99e21c",
# META       "default_lakehouse_name": "SCM_Silver_LH",
# META       "default_lakehouse_workspace_id": "f6eefe30-09e0-42fd-b87d-b4b372c2de7a",
# META       "known_lakehouses": [
# META         {
# META           "id": "ffa69779-942a-4e38-86f2-c60e9c99e21c"
# META         },
# META         {
# META           "id": "f8eb1159-afd1-49f6-afb6-7d3267c1ab39"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "98f863b2-458a-4924-924e-617909254b9a",
# META       "known_warehouses": [
# META         {
# META           "id": "98f863b2-458a-4924-924e-617909254b9a",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "1ef240a0-8e69-4f49-85d0-a897eb15354d",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.sql("CREATE SCHEMA IF NOT EXISTS ops")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eina_df = spark.read.table("SCM_Bronze_LH.sap.EINA")
mvke_df = spark.read.table("SCM_Bronze_LH.sap.MVKE")
material_df = spark.read.table("SCM_Silver_LH.analytics.Material")
vendor_df = spark.read.table("SCM_Silver_LH.ops.Vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

purchase_material = regexp_replace(col("MATNR"), "^0+", "")
purchase_vendor = regexp_replace(col("LIFNR"), "^0+", "")

window_purchase = Window.partitionBy(
    purchase_material,
    purchase_vendor
).orderBy(col("ERDAT").desc())

ranked_purchase_df = eina_df.select(
    col("LOEKZ"),
    purchase_material.alias("PurchaseMaterial"),
    purchase_vendor.alias("PurchaseVendor"),
    row_number().over(window_purchase).alias("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

purchase_info_df = ranked_purchase_df.filter(
    col("rn") == 1
).select(
    "LOEKZ",
    "PurchaseMaterial",
    "PurchaseVendor"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mvke_material = regexp_replace(col("MATNR"), "^0+", "")

window_mvke = Window.partitionBy(
    mvke_material
).orderBy(col("VKORG"))

ranked_mvke_df = mvke_df.select(
    col("MATNR"),
    col("VKORG"),
    col("VMSTA"),
    row_number().over(window_mvke).alias("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ranked_mvke_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_mvke_df = ranked_mvke_df.filter(
    col("rn") == 1
).select(
    regexp_replace(col("MATNR"), "^0+", "").alias("MATNR1"),
    col("VKORG"),
    col("VMSTA")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_mvke_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mat_purchase_df = material_df.join(
    purchase_info_df,
    material_df["Material"] == purchase_info_df["PurchaseMaterial"],
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mat_purchase_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mat_mvke_df = mat_purchase_df.join(
    unique_mvke_df,
    mat_purchase_df["Material"] == unique_mvke_df["MATNR1"],
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mat_mvke_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = mat_mvke_df.join(
    vendor_df,
    mat_mvke_df["PurchaseVendor"] == vendor_df["Vendor"],
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_vendor_df = joined_df.select(
    col("Material"),

    col("Material_Text").cast("string").alias("Material Text"),

    # col("Originating_Plant"),
    # col("Product_Hierarchy"),

    when(col("Material_Type").isin("FERT","HAWA"), "X")
    .otherwise("")
    .alias("Sellable Item"),

    col("VKORG").alias("Sales Org"),
    col("VMSTA").alias("Prod Life Cycle"),

    col("Vendor"),
    col("Name").cast("string").alias("Name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(material_vendor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_vendor_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in material_vendor_df.columns:
    clean_name = c.strip().replace(" ", "_")
    material_vendor_df = material_vendor_df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_vendor_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.material_vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
