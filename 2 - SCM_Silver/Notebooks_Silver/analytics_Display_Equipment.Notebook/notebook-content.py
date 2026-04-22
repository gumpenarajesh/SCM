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

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, coalesce, substring, concat,
    row_number, max as _max, trim, replace
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

equi    = spark.table("SCM_Bronze_LH.sap.EQUI").filter("MANDT = '100'")
equz    = spark.table("SCM_Bronze_LH.sap.EQUZ").filter("MANDT = '100'")
iloa    = spark.table("SCM_Bronze_LH.sap.ILOA").filter("MANDT = '100'")
viqmel  = spark.table("SCM_Bronze_LH.sap.VIQMEL").filter("MANDT = '100'")
jest    = spark.table("SCM_Bronze_LH.sap.JEST").filter("MANDT = '100'")
tj30t   = spark.table("SCM_Bronze_LH.sap.TJ30T").filter("MANDT = '100'").filter("SPRAS = 'E'")

tj02t   = spark.table("SCM_Bronze_LH.sap.TJ02T").filter("SPRAS = 'E'")
kna1    = spark.table("SCM_Bronze_LH.sap.KNA1").filter("MANDT = '100'")
eqkt    = spark.table("SCM_Bronze_LH.sap.EQKT").filter("MANDT = '100'").filter("SPRAS = 'E'")
makt    = spark.table("SCM_Bronze_LH.sap.MAKT").filter("MANDT = '100'").filter("SPRAS = 'E'")
mara    = spark.table("SCM_Bronze_LH.sap.MARA").filter("MANDT = '100'")
bgmkobj = spark.table("SCM_Bronze_LH.sap.BGMKOBJ").filter("MANDT = '100'")
eqbs    = spark.table("SCM_Bronze_LH.sap.EQBS").filter("MANDT = '100'")
qmel    = spark.table("SCM_Bronze_LH.sap.QMEL").filter("MANDT = '100'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tj30t.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(viqmel)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

itobdata = (
    equz
    .join(iloa,
          (equz["MANDT"] == iloa["MANDT"]) &
          (equz["ILOAN"] == iloa["ILOAN"]))
    .select(
        equz["INGRP"],
        equz["KUND1"],
        iloa["KOKRS"],
        iloa["STORT"],
        iloa["EQFNR"],
        iloa["SWERK"],
        iloa["VKORG"],
        equz["EQUNR"]
    )
    .alias("itob")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(itobdata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

itobdata.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# viqmel_cte = (
#     viqmel
#     .join(equi.select("EQUNR").alias("equi_viq"),
#           col("equi_viq.EQUNR") == viqmel["EQUNR"])
#     .groupBy(col("equi_viq.EQUNR"))
#     .agg(_max(viqmel["BEZDT"]).alias("BEZDT"))
#     .alias("viqmel_cte")
# )


viqmel_cte = (
    qmel
    .filter(col("SHN_EQUIPMENT").isNotNull())
    .join(
        equi.select("EQUNR").alias("equi_viq"),
        col("equi_viq.EQUNR") == qmel["SHN_EQUIPMENT"]
    )
    .groupBy(col("equi_viq.EQUNR"))
    .agg(_max(qmel["BEZDT"]).alias("BEZDT"))
    .alias("viqmel_cte")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(viqmel_cte)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

viqmel_cte.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

equi_tj30 = equi.select("EQUNR", "OBJNR").alias("equi_tj30")

tj30t_cte = (
    equi_tj30
    .join(jest.alias("jest_tj30"),
          col("jest_tj30.OBJNR") == col("equi_tj30.OBJNR"))
    .join(tj30t.alias("tj30t_src"),
          (col("jest_tj30.STAT") == col("tj30t_src.ESTAT")))
        #   (col("tj30t_src.STSMA") == "E0001"))
    .groupBy(col("equi_tj30.EQUNR"))
    .agg(
        F.concat_ws("", F.collect_list(col("tj30t_src.TXT04"))).alias("User Status")
    )
    .alias("tj30t_cte")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tj30t_cte.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

equi_tj02 = equi.select("EQUNR", "OBJNR").alias("equi_tj02")

tj02t_cte = (
    equi_tj02
    .join(jest.alias("jest_tj02"),
          col("jest_tj02.OBJNR") == col("equi_tj02.OBJNR"))
    .join(tj02t.alias("tj02t_src"),
          col("jest_tj02.STAT") == col("tj02t_src.ISTAT"))
    .orderBy(col("tj02t_src.ISTAT").desc())
    .groupBy(col("equi_tj02.EQUNR"))
    .agg(
        F.concat_ws(" ", F.collect_list(col("tj02t_src.TXT04"))).alias("System Status"),
        F.concat_ws(",", F.collect_list(col("tj02t_src.TXT30"))).alias("Desc")
    )
    .alias("tj02t_cte")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tj02t_cte.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sap_date(c):
    return (
        when(c.cast("long") == 0, F.to_date(lit("1900-01-01")))
        .otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))
    )

def strip_leading_zeros(c):
    return F.regexp_replace(
        F.ltrim(F.regexp_replace(c, "0", " ")),
        " ", "0"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

equi_a         = equi.alias("equi")
kna1_currcust  = kna1.alias("kna1_currcust")
kna1_cust      = kna1.alias("kna1_cust")
eqkt_a         = eqkt.alias("eqkt")
makt_a         = makt.alias("makt")
mara_a         = mara.alias("mara")
eqbs_a         = eqbs.alias("eqbs")
bgmkobj_vendor = bgmkobj.filter(col("GAART") == "2").alias("bgmkobj_vendor")
bgmkobj_cust   = bgmkobj.filter(col("GAART") == "1").alias("bgmkobj_cust")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    equi_a

    .join(itobdata,
          col("equi.EQUNR") == col("itob.EQUNR"),
          "left")

    .join(kna1_currcust,
          col("equi.KUNDE") == col("kna1_currcust.KUNNR"),
          "left")

    .join(kna1_cust,
          col("itob.KUND1") == col("kna1_cust.KUNNR"),
          "left")

    .join(eqkt_a,
          col("equi.EQUNR") == col("eqkt.EQUNR"),
          "left")

    .join(makt_a,
          col("equi.MATNR") == col("makt.MATNR"),
          "left")

    .join(mara_a,
          col("equi.MATNR") == col("mara.MATNR"),
          "left")

    .join(viqmel_cte,
          col("equi.EQUNR") == col("viqmel_cte.EQUNR"),
          "left")

    .join(tj30t_cte,
          col("equi.EQUNR") == col("tj30t_cte.EQUNR"),
          "left")

    .join(tj02t_cte,
          col("equi.EQUNR") == col("tj02t_cte.EQUNR"),
          "left")

    .join(bgmkobj_vendor,
          col("equi.OBJNR") == col("bgmkobj_vendor.J_OBJNR"),
          "left")

    .join(bgmkobj_cust,
          col("equi.OBJNR") == col("bgmkobj_cust.J_OBJNR"),
          "left")

    .join(eqbs_a,
          col("equi.EQUNR") == col("eqbs.EQUNR"),
          "left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(

    strip_leading_zeros(col("equi.SERNR")).alias("Serial Number"),
    sap_date(col("equi.ERDAT")).alias("Created on"),
    strip_leading_zeros(col("equi.EQUNR")).alias("Equipment"),

    when(col("equi.MATNR").like("00%"),
         F.regexp_replace(
             F.ltrim(F.regexp_replace(col("equi.MATNR"), "0", " ")),
             " ", "0"
         )
    ).otherwise(col("equi.MATNR")).alias("Material"),

    sap_date(col("equi.AEDAT")).alias("Changed on"),
    col("equi.AENAM").alias("Changed by"),
    sap_date(col("equi.ANSDT")).alias("Acquistion date"),
    col("equi.ANSWT").cast(DecimalType(15, 2)).alias("AcquistnValue"),
    col("equi.EQART").alias("Object type"),

    col("itob.KOKRS").alias("CO Area"),
    col("itob.STORT").alias("Location"),
    col("itob.EQFNR").alias("Sort field"),
    col("itob.INGRP").alias("Planner group"),

    strip_leading_zeros(col("itob.KUND1")).alias("Customer"),
    strip_leading_zeros(col("equi.KUNDE")).alias("CurCustomer"),

    col("itob.SWERK").alias("MaintPlant"),
    col("itob.VKORG").alias("Sales Org."),

    col("kna1_currcust.NAME1").alias("Current customer Name"),
    col("kna1_currcust.PSTLZ").alias("Current customer Postal Code"),
    col("kna1_currcust.ORT01").alias("Current customer City"),
    col("kna1_currcust.ORT02").alias("Current customer District"),
    col("kna1_currcust.LAND1").alias("Current customer Country"),
    col("kna1_currcust.REGIO").alias("Current customer Region"),
    col("kna1_currcust.STRAS").alias("Current customer Street"),
    col("kna1_currcust.TELF1").alias("Current customer Telephone 1"),

    col("kna1_cust.TELF1").alias("Telephone 1"),
    col("kna1_cust.ORT01").alias("City"),
    col("kna1_cust.PSTLZ").alias("Postal Code"),
    col("kna1_cust.ORT02").alias("District"),
    col("kna1_cust.STRAS").alias("Street"),
    col("kna1_cust.REGIO").alias("Region"),
    col("kna1_cust.LAND1").alias("Country"),
    col("kna1_cust.NAME1").alias("Name"),

    coalesce(
        F.nullif(F.trim(col("kna1_cust.NAME2")), lit("")),
        F.nullif(F.trim(col("kna1_cust.NAME3")), lit("")),
        F.nullif(F.trim(col("kna1_cust.NAME4")), lit(""))
    ).alias("List Name"),

    col("eqkt.EQKTX").alias("Description of Technical Object"),

    # sap_date(col("bgmkobj_cust.GWLDT")).alias("Cust. Warranty Start"),
    # sap_date(col("bgmkobj_cust.GWLEN")).alias("Cust. Warranty End"),
    # sap_date(col("bgmkobj_vendor.GWLDT")).alias("Vendor Warranty Start"),
    # sap_date(col("bgmkobj_vendor.GWLEN")).alias("Vendor Warranty End"),

    

    when(col("bgmkobj_cust.GWLDT").isNull(), lit("")).otherwise(sap_date(col("bgmkobj_cust.GWLDT"))).alias("Cust. Warranty Start"),
    when(col("bgmkobj_cust.GWLEN").isNull(), lit("")).otherwise(sap_date(col("bgmkobj_cust.GWLEN"))).alias("Cust. Warranty End"),
    when(col("bgmkobj_vendor.GWLDT").isNull(), lit("")).otherwise(sap_date(col("bgmkobj_vendor.GWLDT"))).alias("Vendor Warranty Start"),
    when(col("bgmkobj_vendor.GWLEN").isNull(), lit("")).otherwise(sap_date(col("bgmkobj_vendor.GWLEN"))).alias("Vendor Warranty End"),

    col("mara.SPART").alias("Product Group"),
    col("mara.NORMT").alias("Tradename"),
    # col("equi.ZZUDI").alias("UDI"),
    col("makt.MAKTX").alias("Material Description"),

    col("eqbs.B_WERK").alias("Plant"),
    col("eqbs.B_LAGER").alias("Stor. Location"),
    col("eqbs.KDAUF").alias("Sales Order"),
    col("eqbs.KDPOS").alias("Sales Ord. Item"),

    sap_date(col("viqmel_cte.BEZDT")).alias("Last Service Date"),

    col("tj30t_cte.User Status"),
    col("tj02t_cte.System Status"),

    col("equi.EQUNR").alias("INTEGRATION ID"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import col, trim

final_df = final_df.filter(
    (col("Serial_Number").isNotNull()) &
    (trim(col("Serial_Number")) != "")& (trim(col("Customer")) != "")
)


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

final_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.display_equipment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
