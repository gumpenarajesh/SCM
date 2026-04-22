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

from pyspark.sql import functions as F
from pyspark.sql.functions import col

from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbak = spark.table("SCM_Bronze_LH.sap.vbak").filter("MANDT = '100'")

vbap = spark.table("SCM_Bronze_LH.sap.vbap").filter("MANDT = '100'")

veda = spark.table("SCM_Bronze_LH.sap.veda").filter("MANDT = '100'")

viser02 = spark.table("SCM_Bronze_LH.sap.viser02").filter("MANDT = '100'")

equi = spark.table("SCM_Bronze_LH.sap.equi").filter("MANDT = '100'")

eqkt = spark.table("SCM_Bronze_LH.sap.eqkt").filter("MANDT = '100'").filter("SPRAS='E'")

kna1 = spark.table("SCM_Bronze_LH.sap.kna1").filter("MANDT = '100'")

# vbpa_parvw_zr = spark.table("SCM_Bronze_LH.sap.vbpa_parvw_zr")

pa0001 = spark.table("SCM_Bronze_LH.sap.pa0001").filter("MANDT = '100'")

# vbpa_parvw_ap = spark.table("SCM_Bronze_LH.sap.vbpa_parvw_ap")

adr6 = spark.table("SCM_Bronze_LH.sap.adr6").filter("CLIENT = '100'")

adrc = spark.table("SCM_Bronze_LH.sap.adrc").filter("CLIENT = '100'")

vbkd = spark.table("SCM_Bronze_LH.sap.vbkd").filter("MANDT = '100'")

tvakt = spark.table("SCM_Bronze_LH.sap.tvakt").filter("MANDT = '100'").filter("SPRAS='E'")

mara = spark.table("SCM_Bronze_LH.sap.mara").filter("MANDT = '100'")

bgmkobj = spark.table("SCM_Bronze_LH.sap.bgmkobj").filter("MANDT = '100'")

vbfa_raw = spark.table("SCM_Bronze_LH.sap.vbfa").filter("MANDT = '100'")

vbpa = spark.table("SCM_Bronze_LH.sap.vbpa").filter("MANDT = '100'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def remove_leading_zero(col):
    return F.regexp_replace(col, "^0+", "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sap_date(col):
    return F.when(F.col(col) == 0, F.lit("1900-01-01")) \
            .otherwise(F.to_date(F.col(col).cast("string"), "yyyyMMdd"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_vbfa = Window.partitionBy("VBELN","POSNN").orderBy(F.col("ERDAT").desc())

vbfa = (
    vbfa_raw
    # .filter((F.col("VBTYP_N") == "B") & (F.col("VBTYP_V") == "G"))
    .withColumn("rn", F.row_number().over(window_vbfa))
    .filter(F.col("rn") == 1)
    .select("VBELV","POSNV","VBELN","POSNN","ERDAT")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = (
    vbak.alias("vbak")
    # VEDA: Header level vposn = '000000'
    .join(
        veda.alias("veda"),
        F.col("vbak.vbeln") == F.col("veda.vbeln"),
        #& (F.col("veda.vposn") == F.lit("000000")),
        "inner"
    )
    # VBAP: Item level
    .join(
        vbap.alias("vbap"),
        (F.col("vbak.vbeln") == F.col("vbap.vbeln")) &
        (F.col("vbak.mandt") == F.col("vbap.mandt")),
        "inner"
    )
    # MARA: Material filter
    .join(
        mara.alias("mara"),
        F.col("vbap.matnr") == F.col("mara.matnr"),
        # &
        # (F.col("mara.integration_include") == F.lit(1)),
        "left"
    )
    # VISER02 + Equipment + Text
    .join(
        viser02.alias("viser02"),
        (F.col("viser02.sdaufnr") == F.col("vbap.vbeln")) &
        (F.col("viser02.posnr") == F.col("vbap.posnr")),
        "left"
    )
    .join(
        equi.alias("equi"),
        F.col("equi.equnr") == F.col("viser02.equnr"),
        "left"
    )
    .join(
        eqkt.alias("eqkt"),
        (F.col("eqkt.equnr") == F.col("viser02.equnr")) &
        (F.col("eqkt.spras") == F.lit("E")),
        "left"
    )
    # Customer master
    .join(
        kna1.alias("kna1"),
        F.col("kna1.kunnr") == F.col("vbak.kunnr"),
        "left"
    )
    # --- VBPA (simple) for ZR and AP at header level (posnr = '000000') ---
    .join(
        vbpa.alias("vbpa_zr"),
        (F.col("vbpa_zr.vbeln") == F.col("vbak.vbeln")) &
        # (F.col("vbpa_zr.posnr") == F.lit("000000")) &
        (F.col("vbpa_zr.parvw") == F.lit("ZR")) &
        (F.col("vbpa_zr.mandt") == F.col("vbak.mandt")),
        "left"
    )
    .join(
        vbpa.alias("vbpa_ap"),
        (F.col("vbpa_ap.vbeln") == F.col("vbak.vbeln")) &
        # (F.col("vbpa_ap.posnr") == F.lit("000000")) &
        (F.col("vbpa_ap.parvw") == F.lit("AP")) &
        (F.col("vbpa_ap.mandt") == F.col("vbak.mandt")),
        "left"
    )
    # HR: Employee master via VBPA_ZR.PERNR (only if PERNR is populated for ZR partner)
    .join(
        pa0001.alias("pa0001"),
        (F.col("pa0001.pernr") == F.col("vbpa_zr.pernr")) &
        (F.col("pa0001.endda") == F.lit("99991231")),
        "left"
    )
    # Address/email via VBPA_AP.ADRNR (header address for AP)
    .join(
        adr6.alias("adr6"),
        (F.col("adr6.addrnumber") == F.col("vbpa_ap.adrnr")) &
        (
            (F.col("adr6.persnumber") == F.lit("")) |
            (F.col("adr6.persnumber").isNull())
        ),
        "left"
    )
    .join(
        adrc.alias("adrc"),
        F.col("adrc.addrnumber") == F.col("vbpa_ap.adrnr"),
        "left"
    )
    # Sales document data
    .join(
        vbkd.alias("vbkd"),
        F.col("vbkd.vbeln") == F.col("vbak.vbeln"),
        "left"
    )
    .join(
        tvakt.alias("tvakt"),
        F.col("tvakt.auart") == F.col("vbak.auart"),
        "left"
    )
    .join(
        vbfa.alias("vbfa"),
        (F.col("vbfa.vbeln") == F.col("vbap.vbeln")) &
        (F.col("vbfa.posnn") == F.col("vbap.posnr")),
        "left"
    )
    .join(
        bgmkobj.alias("bgmkobj"),
        (F.col("bgmkobj.j_objnr") == F.col("equi.objnr")) &
        (F.col("bgmkobj.gaart") == F.lit("2")),
        "left"
    )
    # VEDA at item level
    .join(
        veda.alias("veda_item"),
        (F.col("vbak.vbeln") == F.col("veda_item.vbeln")) &
        (F.col("veda_item.vposn") == F.col("vbap.posnr")),
        "left"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_df = df.select(

# Sales org + plant
F.col("vbak.vkorg").alias("Sales Org."),
F.col("vbap.werks").alias("Plant"),

# Created date
sap_date("vbak.erdat").alias("Created on"),

F.col("vbak.ernam").alias("Created by"),

# Sales document
remove_leading_zero(F.col("vbak.vbeln")).alias("Sales Document"),

F.col("vbak.ktext").alias("Description"),

# Item
remove_leading_zero(F.col("vbap.posnr")).alias("Item"),

F.col("vbap.arktx").alias("Item Description"),

remove_leading_zero(F.col("vbap.vgbel")).alias("Reference doc."),

remove_leading_zero(F.col("vbap.vgpos")).alias("Reference item"),

F.col("vbak.auart").alias("Sales Doc. Type"),

F.col("tvakt.bezei").alias("Sales Doc. Type Text"),

F.col("vbap.pstyv").alias("Item category"),

F.col("vbap.spart").alias("Product group"),

F.col("vbap.prodh").alias("Prod.hierarchy"),

# Material
F.when(
    F.col("viser02.matnr").startswith("00"),
    remove_leading_zero(F.col("viser02.matnr"))
).otherwise(F.col("viser02.matnr")).alias("Material"),

F.when(
    F.col("vbap.matwa").startswith("00"),
    remove_leading_zero(F.col("vbap.matwa"))
).otherwise(F.col("vbap.matwa")).alias("MaterialEntered"),

# Equipment
remove_leading_zero(F.col("viser02.equnr")).alias("Equipment"),

F.col("eqkt.eqktx").alias("Equipment Text"),

remove_leading_zero(F.col("viser02.sernr")).alias("Serial Number"),

# Contract dates
sap_date("veda.vbegdat").alias("Contract start"),

sap_date("veda.venddat").alias("Contract end"),

# Sales rep
F.col("pa0001.pernr").alias("Sales rep"),

F.col("pa0001.ename").alias("Sales rep Text"),

# Prices
F.col("vbap.netpr").cast("decimal(15,2)").alias("Net price"),

F.col("vbap.netwr").cast("decimal(15,2)").alias("Net value"),

F.col("vbak.waerk").alias("Doc. Currency"),

F.col("vbap.waerk").alias("Doc. Currency document"),

# Customer reference
F.col("vbak.bsark").alias("Pur. ord. type"),

F.col("vbak.bstnk").alias("Cust.ref.no"),

F.col("vbap.posex").alias("PO item"),

sap_date("vbak.bstdk").alias("Cust.ref. date"),

# sap_date("vbak.zzpmdate").alias("PM Date"),

# Contact details
F.col("vbak.telf1").alias("Telephone"),

F.col("adrc.name1").alias("Contact"),

F.col("adrc.tel_number").alias("Contact Phone"),

F.col("adr6.smtp_addr").alias("Contact Email"),

# Customer
remove_leading_zero(F.col("vbak.kunnr")).alias("Sold-to party"),

F.col("kna1.name1").alias("Name"),

F.col("kna1.name2").alias("Name 2"),

F.col("kna1.stras").alias("Street"),

F.col("kna1.ort01").alias("City"),

F.col("kna1.regio").alias("Region"),

F.col("kna1.pstlz").alias("Postal Code"),

# Sales organization fields
F.col("vbak.vtweg").alias("Distr. Channel"),

F.col("vbak.vkgrp").alias("Sales group"),

F.col("vbak.vkbur").alias("Sales office"),

# Equipment flag
# F.col("equi.zzrmtcr").alias("Remote Care"),

# Billing block
F.col("vbak.faksk").alias("Billing block document"),

F.col("vbap.faksp").alias("Billing block item"),

# Payment terms
F.col("vbkd.zterm").alias("Payt Terms"),

# Material info
F.col("vbap.matkl").alias("Material group"),

F.col("vbap.charg").alias("Batch"),

# Document category
F.col("vbak.vbtyp").alias("Document cat."),

# Quantities
F.col("vbap.zmeng").cast("decimal(15,2)").alias("Target quantity"),

F.col("vbap.zieme").alias("Target qty UoM"),

F.col("vbap.umzin").cast("decimal(15,2)").alias("Convers. factor"),

# Units
F.col("vbap.meins").alias("Base Unit"),

F.col("vbap.ean11").alias("EAN/UPC"),

# Profit center
F.col("vbap.prctr").alias("Profit Center"),

# Configuration
F.col("vbap.cuobj").alias("Configuration"),

# Contract rules
F.col("veda.vlaufz").alias("Validity period"),

F.col("veda.vlauez").alias("Unit val.period"),

F.col("veda.vlaufk").alias("Val.per.cat."),

sap_date("veda.vinsdat").alias("Installat.date"),

sap_date("veda.vabndat").alias("Acceptance date"),

sap_date("veda.vuntdat").alias("Contract signed"),

# Cancellation info
F.col("veda.vkuesch").alias("Canc.proced."),

sap_date("veda.veindat").alias("Receipt of canc"),

sap_date("veda.vwundat").alias("Req.canc.date"),

F.col("veda.vkuepar").alias("Cancellat.party"),

F.col("veda.vkuegru").alias("Reason f.canc."),

F.col("veda.vbelkue").alias("Cancel.document"),

sap_date("veda.vbedkue").alias("Canc.doc.date"),

# Guarantee start
sap_date("bgmkobj.gwldt").alias("Begin guarantee"),

# Previous document
F.col("vbfa.vbelv").alias("Preceding Doc."),

# Contract item start
F.coalesce(
    sap_date("veda_item.vbegdat"),
    sap_date("veda.vbegdat")
).alias("ContractItem start"),

# Contract item end
F.coalesce(
    sap_date("veda_item.venddat"),
    sap_date("veda.venddat")
).alias("ContractItem end"),

# Integration ID
F.concat(
    F.col("vbap.vbeln"),
    F.col("vbap.posnr"),
    F.col("viser02.obknr"),
    F.col("viser02.obzae")
).alias("INTEGRATION_ID")

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in contract_df.columns:
    clean_name = c.strip().replace(" ", "_")
    contract_df = contract_df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.contract")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.contract LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

contract_test=contract_df.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(contract_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
