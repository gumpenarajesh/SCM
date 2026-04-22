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
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

# Load tables
vbak = spark.table("sap.VBAK")
vbap = spark.table("sap.VBAP")
kna1 = spark.table("sap.KNA1")
knvh = spark.table("sap.KNVH")
sales_org = spark.table("SCM_Silver_LH.analytics.Sales_Organization")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_doc_type_item = (
    vbak.alias("vbak")
    .join(vbap.alias("vbap"), col("vbak.VBELN") == col("vbap.VBELN"), "inner")
    # .filter(
    #     (col("vbap.ABGRU") == "") &
    #     (col("vbap.KTGRM").isin("01", "S5", "04")) &
    #     (col("vbak.AUART").isin(
    #         "ZCA","ZCH","ZCR","WK1","ZCAB","ZCHB","WK1B","ZR"
    #     ))
    # )
    .select(
        col("vbak.VBELN"),
        col("vbak.KTEXT"),
        col("vbap.POSNR"),
        col("vbap.ARKTX"),
        col("vbak.AUART"),
        col("vbak.AEDAT"),
        col("vbak.VKORG"),
        col("vbak.SPART"),
        col("vbak.KUNNR"),
        col("vbak.NETWR"),
        col("vbak.WAERK"),
        col("vbak.GUEBG"),
        col("vbak.GUEEN"),
        # col("vbap.ZZVISMMR"),
        # col("vbap.ZZEXPCUSPD"),
        # col("vbap.ZZNUMTST"),
        col("vbap.MATNR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_doc_type_item.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

service_parts_mat_item = (
    vbak.alias("vbak")
    .join(vbap.alias("vbap"), col("vbak.VBELN") == col("vbap.VBELN"), "inner")
    # .filter(
    #     (col("vbap.MATNR").like("9%")) &
    #     (col("vbap.ABGRU") == "")
    # )
    .select(
        col("vbak.VBELN"),
        col("vbak.KTEXT"),
        col("vbap.POSNR"),
        col("vbap.ARKTX"),
        col("vbak.AUART"),
        col("vbak.AEDAT"),
        col("vbak.VKORG"),
        col("vbap.SPART"),
        col("vbak.KUNNR"),
        col("vbap.NETWR"),
        col("vbak.WAERK"),
        col("vbak.GUEBG"),
        col("vbak.GUEEN"),
        # col("vbap.ZZVISMMR"),
        # col("vbap.ZZEXPCUSPD"),
        # col("vbap.ZZNUMTST"),
        col("vbap.MATNR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

service_parts_mat_item.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

service_not_in_sales = (
    service_parts_mat_item.alias("sp")
    .join(
        sales_doc_type_item.alias("sd"),
        (col("sp.VBELN") == col("sd.VBELN")) &
        (col("sp.POSNR") == col("sd.POSNR")),
        "left"
    )
    .filter(col("sd.VBELN").isNull())
    .select("sp.*")
)

un_service_parts_sales_doc = sales_doc_type_item.unionByName(service_not_in_sales)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_filtered = (
    knvh.filter(
        (col("DATBI") == "99991231") &
        (col("HITYP") == "Z")
    )
    .select("KUNNR", "DATAB", "DATBI", "HKUNNR")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    un_service_parts_sales_doc.alias("u")
    .join(knvh_filtered.alias("knvh"), col("u.KUNNR") == col("knvh.KUNNR"), "left")
    .join(kna1.alias("kna1"), col("knvh.HKUNNR") == col("kna1.KUNNR"), "left")
    .join(kna1.alias("kna"), col("u.KUNNR") == col("kna.KUNNR"), "left")
    .join(sales_org.alias("so"), col("u.VKORG") == col("so.Sales_Org"), "left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# -------------------------------------------------------
# FINAL TRANSFORMATIONS
# -------------------------------------------------------
result = (
    df.filter(col("u.GUEEN") >= "20200101")
    .select(
        regexp_replace(col("u.VBELN"), "^0+", "").alias("Sales Document"),
        col("u.KTEXT").alias("Description"),
        regexp_replace(col("u.POSNR"), "^0+", "").alias("Item"),
        col("u.ARKTX").alias("Item Description"),

        when(col("u.MATNR").like("00%"),
             regexp_replace(col("u.MATNR"), "^0+", "")
        ).otherwise(col("u.MATNR")).alias("Material"),

        col("u.AUART").alias("Sales Doc.Type"),
        col("u.VKORG").alias("Sales Org."),
        # col("so.OpCo"),

        regexp_replace(col("u.KUNNR"), "^0+", "").alias("Sold-to party"),

        col("kna.NAME1").alias("Name"),
        col("kna.ORT01").alias("City"),
        col("kna1.NAME1").alias("Pal1 Customer Name"),

        when(col("u.GUEBG") == 0, lit("1900-01-01"))
        .otherwise(to_date(col("u.GUEBG").cast("string"), "yyyyMMdd"))
        .alias("Valid from"),

        when(col("u.GUEEN") == 0, lit("1900-01-01"))
        .otherwise(to_date(col("u.GUEEN").cast("string"), "yyyyMMdd"))
        .alias("Valid to"),

        col("u.SPART").alias("Product group"),

        col("u.NETWR").cast(DecimalType(15,1)).alias("Net value"),
        # col("u.ZZVISMMR").cast(DecimalType(15,2)).alias("Min Month Paymt"),
        # col("u.ZZNUMTST").alias("Number of tests"),
        # col("u.ZZEXPCUSPD").cast(DecimalType(15,2)).alias("Exp.cust Spend"),

        concat(col("u.VBELN"), col("u.POSNR")).alias("Integration ID")
    )
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
  .saveAsTable("SCM_Silver_LH.analytics.custom_contract")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.custom_contract LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
