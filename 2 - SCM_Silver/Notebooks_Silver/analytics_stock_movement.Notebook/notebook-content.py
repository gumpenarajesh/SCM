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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Load tables
mseg = spark.table("sap.MSEG").filter("MANDT = '100'")
mara = spark.table("sap.MARA").filter("MANDT = '100'")
makt = spark.table("sap.MAKT").filter("MANDT = '100'").filter("SPRAS='E'")
mbew = spark.table("sap.MBEW").filter("MANDT = '100'")
marc = spark.table("sap.MARC").filter("MANDT = '100'")
mard = spark.table("sap.MARD").filter("MANDT = '100'")
mkpf = spark.table("sap.MKPF").filter("MANDT = '100'")
tspat = spark.table("sap.TSPAT").filter("MANDT = '100'")
kna1 = spark.table("sap.KNA1").filter("MANDT = '100'")
ekpo = spark.table("sap.EKPO").filter("MANDT = '100'")
ekko = spark.table("sap.EKKO").filter("MANDT = '100'")
lfa1 = spark.table("sap.LFA1").filter("MANDT = '100'")
t156t = spark.table("sap.T156T").filter("MANDT = '100'")
ser03 = spark.table("sap.SER03").filter("MANDT = '100'")
objk = spark.table("sap.OBJK").filter("MANDT = '100'")
pkhd = spark.table("sap.PKHD").filter("MANDT = '100'")
vbfa = spark.table("sap.VBFA").filter("MANDT = '100'")
vbak = spark.table("sap.VBAK").filter("MANDT = '100'")
vbpa_df = spark.table("sap.VBPA").filter("MANDT = '100'").filter("PARVW = 'WE'")
# vbpa_ship = spark.table("sap.VBPA_SHIP_TO")
adrc = spark.table("sap.ADRC").filter("CLIENT='100'")





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.vbpa_ship_to LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_pkhd = Window.partitionBy("MATNR", "WERKS").orderBy("PKNUM")

pkhd_df = (
    pkhd
    .withColumn("RN", row_number().over(w_pkhd))
    .filter(col("RN") == 1)
    .select("MATNR", "WERKS", "BEHMG")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_makt = Window.partitionBy("MATNR").orderBy("MATNR")

makt_df = (
    makt
    .filter(~col("MAKTX").like("%delete%"))
    .withColumn("RN", row_number().over(w_makt))
    .filter(col("RN") == 1)
    .select("MATNR", "MAKTX")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_mbew = Window.partitionBy("MATNR", "BWKEY", "BWTAR") \
    .orderBy(concat(col("LFGJA"), col("LFMON")).desc())

mbew_df = (
    mbew
    .filter(col("LVORM") == "")
    .withColumn("RN", row_number().over(w_mbew))
    .filter(col("RN") == 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

marc_df = (
    marc
    .filter(col("LVORM") == "")
    .groupBy("MATNR", "WERKS")
    .agg(max("DISPO").alias("DISPO"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_join = vbfa.alias("vbf").join(
    vbak.alias("vbk"),
    col("vbf.VBELV") == col("vbk.VBELN"),
    "inner"
)

w_vbfa = Window.partitionBy("vbf.VBELN", "vbf.POSNN") \
    .orderBy(col("vbf.VBELV").desc())

vbfa_df = (
    vbfa_join
    # .filter(
    #     (col("vbf.VBTYP_N") == "J") &
    #     ((col("vbf.VBTYP_V") == "C") | (col("vbf.VBTYP_V") == "E")) &
    #     (col("vbk.VDATU") != "")
    # )
    .withColumn("RN", row_number().over(w_vbfa))
    .filter(col("RN") == 1)
    .select(
        col("vbf.VBELN").alias("VBELN"),
        col("vbf.POSNN").alias("POSNN"),
        col("vbf.VBELV").alias("VBELV"),
        col("vbk.VBELN").alias("VBAKVBELN"),
        col("vbk.VDATU").alias("VDATU")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = mseg.alias("mseg") \
    .join(mara.alias("mara"),
          (col("mara.MATNR") == col("mseg.MATNR")) 
        #   (col("mara.INTEGRATION_INCLUDE") == "1"),
          ,"left") \
    .join(tspat.alias("tspat"),
          col("tspat.SPART") == col("mara.SPART"),
          "left") \
    .join(kna1.alias("kna1"),
          col("kna1.KUNNR") == col("mseg.KUNNR"),
          "left") \
    .join(marc_df.alias("marc"),
          (col("marc.MATNR") == col("mseg.MATNR")) &
          (col("marc.WERKS") == col("mseg.WERKS")),
          "left") \
    .join(mard.alias("mard"),
          (col("mard.MATNR") == col("mseg.MATNR")) &
          (col("mard.WERKS") == col("mseg.WERKS")) &
          (col("mard.LGORT") == col("mseg.LGORT")) &
          (col("mard.LVORM") == ""),
          "left") \
    .join(mbew_df.alias("mbew"),
          (col("mbew.MATNR") == col("mseg.MATNR")) &
          (col("mbew.BWKEY") == col("mseg.WERKS")),
          "left") \
    .join(mkpf.alias("mkpf"),
          col("mkpf.MBLNR") == col("mseg.MBLNR"),
          "left") \
    .join(makt_df.alias("makt"),
          col("makt.MATNR") == col("mseg.MATNR"),
          "left") \
    .join(vbfa_df.alias("vbfa"),
          (col("vbfa.VBELN") == col("mseg.VBELN_IM")) &
          (col("vbfa.POSNN") == col("mseg.VBELP_IM")),
          "left") \
    .join(vbpa_df.alias("ship"),
          (col("ship.VBELN") == col("vbfa.VBAKVBELN")), 
      #     (col("ship.POSNR") == "000000"),
          "left") \
    .join(ekpo.alias("ekpo"),
          (col("ekpo.EBELN") == col("mseg.EBELN")) &
          (col("ekpo.EBELP") == col("mseg.EBELP")),
          "left") \
    .join(ekko.alias("ekko"),
          col("ekko.EBELN") == col("mseg.EBELN"),
          "left") \
    .join(lfa1.alias("lfa1"),
          col("lfa1.LIFNR") == col("ekko.LIFNR"),
          "left") \
    .join(pkhd_df.alias("pkhd"),
          (col("pkhd.MATNR") == col("mseg.MATNR")) &
          (col("pkhd.WERKS") == col("mseg.WERKS")),
          "left") \
    .join(ser03.alias("ser03"),
          (col("ser03.MBLNR") == col("mseg.MBLNR")) &
          (col("ser03.MJAHR") == col("mseg.MJAHR")) &
          (col("ser03.ZEILE") == col("mseg.ZEILE")),
          "left") \
    .join(objk.alias("objk"),
          col("objk.OBKNR") == col("ser03.OBKNR"),
          "left") \
    .join(t156t.alias("t156t"),
          (col("t156t.BWART") == col("mseg.BWART")) &
          (col("t156t.SOBKZ") == col("mseg.SOBKZ")),
          "left") \
    .join(adrc.alias("adrc"),
          col("adrc.ADDRNUMBER") == col("ship.ADRNR"),
          "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(

    # Amount
    when(
    col("mbew.PEINH") >= 1,
    (
        col("mseg.MENGE").cast("decimal(15,2)") *
        round(
            col("mbew.VERPR").cast("decimal(15,2)") /
            col("mbew.PEINH").cast("decimal(15,2)"),
            2
        ).cast("decimal(15,2)")
    )
).alias("Amount"),

    col("mara.LABOR").alias("Article type"),
    col("mseg.CHARG").alias("Batch"),
    col("mard.LGPBE").alias("Storage Bin"),

    # Sold to
    regexp_replace(col("mseg.KUNNR"), "^0+", "").alias("SoldToCustomer"),
    col("kna1.NAME1").alias("SoldToCustomerName"),

    col("makt.MAKTX").alias("Description"),

    # Mat doc item
    when(col("mseg.ZEILE").startswith("00"),
         regexp_replace(col("mseg.ZEILE"), "^0+", "")
    ).otherwise(col("mseg.ZEILE")).alias("Mat. Doc.Item"),

    # Entered on
    when(col("mkpf.CPUDT") == 0, lit("1900-01-01"))
    .otherwise(to_date(col("mkpf.CPUDT").cast("string"), "yyyyMMdd"))
    .alias("Entered on"),

    col("mara.BRGEW").cast("decimal(15,2)").alias("Gross Weight"),

    col("mseg.EBELP").alias("Item"),
    col("pkhd.BEHMG").cast("decimal(15,2)").alias("Kanban Quantity"),

    col("mseg.MBLNR").alias("Material Doc."),

    # Material
    when(col("mseg.MATNR").startswith("00"),
         regexp_replace(col("mseg.MATNR"), "^0+", "")
    ).otherwise(col("mseg.MATNR")).alias("Material"),

    col("mkpf.USNAM").alias("User name"),
    col("mseg.BWART").alias("Movement Type"),

    col("t156t.BTEXT").alias("Mvt Type Text"),

    col("mseg.WERKS").alias("Plant"),
    col("mseg.EBELN").alias("Purchase Order"),
    col("ekko.ERNAM").alias("Created by"),

    col("mara.SPART").alias("Product group"),
    col("tspat.VTEXT").alias("Product group Name"),

    # Posting Date
    when(col("mkpf.BUDAT") == 0, lit("1900-01-01"))
    .otherwise(to_date(col("mkpf.BUDAT").cast("string"), "yyyyMMdd"))
    .alias("Posting Date"),

    col("mseg.MENGE").cast("decimal(15,2)").alias("Quantity"),

    col("mkpf.XBLNR").alias("Reference"),
    col("mseg.RSNUM").alias("Reserv.No."),

    regexp_replace(col("vbfa.VBAKVBELN"), "^0+", "").alias("Sales Document"),

    col("objk.SERNR").alias("Serial Number"),
    col("mseg.SOBKZ").alias("Special Stock"),

    # Ship To logic
    when(col("ship.KUNNR").isNotNull(),
         regexp_replace(col("ship.KUNNR"), "^0+", "")
    ).otherwise(
        when(col("ekpo.KUNNR").isNotNull(), col("ekpo.KUNNR"))
        .otherwise(col("mseg.WEMPF"))
    ).alias("Ship To Customer"),

    coalesce(col("kna1.NAME1"), col("adrc.NAME1")).alias("Ship To Name"),

    col("kna1.ORT01").alias("City"),
    col("kna1.LAND1").alias("Country"),
    col("kna1.PSTLZ").alias("Postal Code"),
    col("kna1.REGIO").alias("Region"),

    col("mseg.INSMK").alias("Stock Type"),
    col("mseg.LGORT").alias("Stor. Location"),

    # Time
    to_timestamp(
        concat_ws(":",
                  substring(col("mkpf.CPUTM"), 1, 2),
                  substring(col("mkpf.CPUTM"), 3, 2),
                  substring(col("mkpf.CPUTM"), 5, 2)
        )
    ).alias("Entered at"),

    col("mara.GEWEI").alias("Weight unit"),
    col("marc.DISPO").alias("MRP Controller"),

    # Request date
    when(col("vbfa.VDATU") == 0, lit("1900-01-01"))
    .otherwise(to_date(col("vbfa.VDATU").cast("string"), "yyyyMMdd"))
    .alias("Request.dlv.dt"),

    col("ekko.LIFNR").alias("Vendor"),
    col("lfa1.NAME1").alias("Vendor Name"),

    col("mseg.VBELN_IM").alias("Delivery"),
    col("mseg.WAERS").alias("Currency"),

    concat(
        col("mseg.MBLNR"),
        col("mseg.ZEILE"),
        col("mseg.CHARG"),
        col("objk.SERNR")
    ).alias("INTEGRATION_ID")
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

from pyspark.sql.functions import col, trim

final_df = final_df.filter(
    (col("Sales_Document").isNotNull()) 
    
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
  .saveAsTable("SCM_Silver_LH.analytics.stock_movement")

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

display(vbfa_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
