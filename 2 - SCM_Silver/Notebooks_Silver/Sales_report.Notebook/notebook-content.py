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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "1ef240a0-8e69-4f49-85d0-a897eb15354d",
# META       "known_warehouses": [
# META         {
# META           "id": "1ef240a0-8e69-4f49-85d0-a897eb15354d",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "98f863b2-458a-4924-924e-617909254b9a",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
 
# Load tables
vbak = spark.table("sap.vbak").filter("MANDT='100'")
vbap = spark.table("sap.vbap").filter("MANDT='100'")
vbrp = spark.table("sap.vbrp").filter("MANDT='100'")
vbrk = spark.table("sap.vbrk").filter("MANDT='100'")
mara = spark.table("sap.mara").filter("MANDT='100'")
makt = spark.table("sap.makt").filter("MANDT='100'")
kna1 = spark.table("sap.kna1").filter("MANDT='100'")
t179t = spark.table("SCM_Bronze_LH.sap.t179t").filter("MANDT='100'").filter("SPRAS ='E'")
ph   = spark.table("SCM_Silver_LH.analytics.product_hierarchy")
zregion =spark.table("SCM_Silver_LH.analytics.Region")

def remove_dots(col_name):
    # Uses regex to replace all literal dots with an empty string
    return F.regexp_replace(F.col(col_name), "\\.", "")


 
# Helper function to remove leading zeros
def trim_leading_zeros(col):
    return F.expr(f"substring({col}, locate(regexp_extract({col}, '[^0]', 0), {col}))")

ph_clean = ph.withColumn(
    "Product_Hierarchy", 
    remove_dots("Product_Hierarchy")
)
# Joins
df = (
    vbak.alias("vbak")
    .join(vbap.alias("vbap"), F.col("vbak.VBELN") == F.col("vbap.VBELN"), "left")
    .join(
        vbrp.alias("vbrp"),
        (F.col("vbak.VBELN") == F.col("vbrp.AUBEL")) &
        (F.col("vbap.POSNR") == F.col("vbrp.AUPOS")),
        "left"
    )
    .join(vbrk.alias("vbrk"), F.col("vbrk.VBELN") == F.col("vbrp.VBELN"), "left")
    .join(mara.alias("mara"), F.col("vbap.MATNR") == F.col("mara.MATNR"), "left")
    .join(makt.alias("makt"), F.col("mara.MATNR") == F.col("makt.MATNR"), "left")
    .join(kna1.alias("kna1"), F.col("kna1.KUNNR") == F.col("vbak.KUNNR"), "left")
    # .join(t179t.alias("t179t"),F.col("mara.PRDHA") == F.col("t179t.PRODH"), "left")
    .join(ph_clean.alias("ph1"),F.col("mara.PRDHA") == F.col("ph1.Product_Hierarchy"),"left")
    .join(zregion.alias("reg"),F.col("kna1.LAND1") == F.col("reg.LAND1"),"left")
)
 
# Filters
df_filtered = df.filter(
    (F.col("vbak.MANDT") == "100") &
    (F.col("vbap.MANDT") == "100") &
    (F.col("mara.MANDT") == "100") &
    (F.col("kna1.MANDT") == "100") &
    (F.col("vbak.VBELN").between("0206000100", "0206000500") )&    (F.col("vbrp.VBELN") != "") &
    (F.col("makt.SPRAS") == "E")
    

)
 
# Select & Transform
result = df_filtered.select(
    F.col("vbak.VKORG").alias("Sales Org."),
    F.regexp_replace("vbap.VBELN", "^0+", "").alias("Sales Document"),
    F.regexp_replace("vbap.POSNR", "^0+", "").alias("Item"),
 
    F.when(F.col("vbak.ERDAT") == 0, F.lit("1900-01-01"))
     .otherwise(F.to_date(F.col("vbak.ERDAT").cast("string"), "yyyyMMdd"))
     .alias("Order Creation Date"),
 
    F.col("vbak.AUART").alias("Document Type"),
 
    F.regexp_replace("vbrp.VBELN", "^0+", "").alias("Billing Document"),
    F.regexp_replace("vbrp.POSNR", "^0+", "").alias("Billing Item"),
 
    F.when(F.col("vbrk.FKDAT") == 0, F.lit("1900-01-01"))
     .otherwise(F.to_date(F.col("vbrk.FKDAT").cast("string"), "yyyyMMdd"))
     .alias("Billing Date"),
 
    F.regexp_replace("vbap.MATNR", "^0+", "").alias("Material"),
    F.col("makt.MAKTX").alias("Material Text"),
 
    F.col("vbap.NETWR").alias("Net Value"),
    F.col("vbap.WAERK").alias("SD Document Currency"),
    F.col("mara.PRDHA").alias("Product Hierarchy"),
    F.col("ph1.PH_Product_Line_Text").alias("PH_Product_Line_Text"),
    # F.col("t179t.VTEXT").alias("PH_Product_Line_Text"),
    F.col("reg.REGION").alias("Region"),
    F.col("kna1.LAND1").alias("Country"),
    F.col("reg.TEXT").alias("Country_Text")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in result.columns:
    clean_name = c.strip().replace(" ", "_")
    result = result.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.sales_report")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
