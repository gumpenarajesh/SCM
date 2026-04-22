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

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# Load source tables
# =========================================================
vbap = spark.sql("SELECT * FROM sap.vbap")
vbfa_src = spark.sql("SELECT * FROM sap.vbfa")
vbrk_src = spark.sql("SELECT * FROM sap.vbrk")

makt_src = spark.sql("SELECT * FROM sap.makt")
mbew_src = spark.sql("SELECT * FROM sap.mbew")
mara_src = spark.sql("SELECT * FROM sap.mara")
kna1_src = spark.sql("SELECT * FROM sap.kna1")
pa0001_src = spark.sql("SELECT * FROM sap.pa0001")
vbkd_src = spark.sql("SELECT * FROM sap.vbkd")
tvtyt_src = spark.sql("SELECT * FROM sap.tvtyt")

# Use sap.backlog directly
backlog = spark.sql("SELECT * FROM sap.backlog")

# =========================================================
# Helper functions
# =========================================================
def trim_leading_zeros(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), "^0+", "")

def sap_date_nullable(col_name):
    return F.when(
        F.col(col_name).isNull() | (F.col(col_name) == 0),
        F.lit(None).cast("date")
    ).otherwise(
        F.to_date(F.col(col_name).cast("string"), "yyyyMMdd")
    )

# =========================================================
# MAKT CTE
# =========================================================
makt_window = Window.partitionBy("MATNR").orderBy("MATNR")

MAKT = (
    makt_src
    .filter(~F.col("MAKTX").like("%delete%"))
    .withColumn("RN", F.row_number().over(makt_window))
)

MAKT_1 = (
    MAKT
    .filter(F.col("RN") == 1)
    .select("MATNR", "MAKTX")
)

# =========================================================
# MBEW CTE
# =========================================================
mbew_join = (
    mbew_src.alias("MBEW")
    .join(
        vbap.alias("VBAP"),
        (F.col("MBEW.MATNR") == F.col("VBAP.MATNR")) &
        (F.col("MBEW.BWKEY") == F.col("VBAP.WERKS")),
        "inner"
    )
    .filter(
        (F.col("MBEW.LVORM") == "") &
        (F.col("VBAP.PSTYV") == "TAN")
    )
    .select(
        F.col("MBEW.MATNR").alias("MATNR"),
        F.col("MBEW.BWKEY").alias("BWKEY"),
        F.col("MBEW.LVORM").alias("LVORM"),
        F.col("MBEW.BWTAR").alias("BWTAR"),
        F.col("MBEW.BKLAS").alias("BKLAS"),
        F.col("MBEW.VPRSV").alias("VPRSV"),
        F.col("MBEW.PEINH").alias("PEINH"),
        F.col("MBEW.VERPR").alias("VERPR"),
        F.col("MBEW.STPRS").alias("STPRS"),
        F.col("MBEW.LBKUM").alias("LBKUM"),
        F.col("MBEW.SALK3").alias("SALK3"),
        F.col("MBEW.LFGJA").alias("LFGJA"),
        F.col("MBEW.LFMON").alias("LFMON")
    )
)

mbew_window = Window.partitionBy("MATNR", "BWKEY", "BWTAR").orderBy(
    F.concat(F.col("LFGJA"), F.col("LFMON")).desc()
)

MBEW = (
    mbew_join
    .withColumn("RN", F.row_number().over(mbew_window))
    .filter(F.col("RN") == 1)
    .drop("RN", "LFGJA", "LFMON")
)

# =========================================================
# VBFA CTE - Intercompany invoice
# =========================================================
vbfa_order_cols = []
if "ERDAT" in vbfa_src.columns:
    vbfa_order_cols.append(F.col("VFA.ERDAT").desc())
vbfa_order_cols.append(F.col("VFA.VBELN").desc())

vbfa_window = Window.partitionBy("VFA.VBELV", "VFA.POSNV").orderBy(*vbfa_order_cols)

VBFA = (
    vbfa_src.alias("VFA")
    .join(
        backlog.alias("BKL"),
        (F.col("BKL.VBELN") == F.col("VFA.VBELV")) &
        (F.col("BKL.POSNR") == F.col("VFA.POSNV")),
        "inner"
    )
    .filter(F.col("VFA.VBTYP_N") == "5")
    .withColumn("RN", F.row_number().over(vbfa_window))
    .select(
        F.col("VFA.VBELV").alias("VBELV"),
        F.col("VFA.POSNV").alias("POSNV"),
        F.col("VFA.VBELN").alias("INTCO_VBELN"),
        (
            F.col("VFA.POSNN") if "POSNN" in vbfa_src.columns
            else F.lit(None)
        ).alias("INTCO_POSNN"),
        (
            F.col("VFA.ERDAT") if "ERDAT" in vbfa_src.columns
            else F.lit(None)
        ).alias("ERDAT"),
        F.col("RN")
    )
)

# =========================================================
# VBRK CTE for Intercompany invoice date
# =========================================================
vbrk_order_cols = []
if "ERDAT" in vbrk_src.columns:
    vbrk_order_cols.append(F.col("VBRK.ERDAT").desc())
vbrk_order_cols.append(F.col("VBRK.VBELN").desc())

vbrk_w = Window.partitionBy("VBRK.VBELN").orderBy(*vbrk_order_cols)

VBRK = (
    vbrk_src.alias("VBRK")
    .join(
        VBFA.alias("VBFA"),
        F.col("VBRK.VBELN") == F.col("VBFA.INTCO_VBELN"),
        "inner"
    )
    .withColumn("RN", F.row_number().over(vbrk_w))
    .select(
        F.col("VBRK.VBELN").alias("VBELN"),
        (
            F.col("VBRK.FKDAT") if "FKDAT" in vbrk_src.columns
            else F.lit(None)
        ).alias("FKDAT"),
        (
            F.col("VBRK.ERDAT") if "ERDAT" in vbrk_src.columns
            else F.lit(None)
        ).alias("ERDAT"),
        F.col("RN")
    )
)

# =========================================================
# Final result: analytics.Sales Order Returns Detail
# =========================================================
result = (
    backlog.alias("ZXVB1")

    .join(
        mara_src.alias("MARA"),
        F.col("ZXVB1.MATNR") == F.col("MARA.MATNR"),
        "left"
    )

    .join(
        MAKT_1.alias("MAKT"),
        F.col("MAKT.MATNR") == F.col("ZXVB1.MATNR"),
        "left"
    )

    .join(
        kna1_src.alias("KNA1"),
        F.col("ZXVB1.KUNNR") == F.col("KNA1.KUNNR"),
        "left"
    )

    .join(
        MBEW.alias("MBEW"),
        (F.col("MBEW.MATNR") == F.col("ZXVB1.MATNR")) &
        (F.col("MBEW.BWKEY") == F.col("ZXVB1.WERKS")) &
        (F.col("MBEW.LVORM") == ""),
        "left"
    )

    .join(
        pa0001_src.alias("PA0001"),
        (F.col("PA0001.PERNR") == F.col("ZXVB1.PZPZR")) &
        (F.col("PA0001.ENDDA") == "99991231"),
        "left"
    )

    .join(
        pa0001_src.alias("ZI_PA0001"),
        (F.col("ZI_PA0001.PERNR") == F.col("ZXVB1.PZPZI")) &
        (F.col("ZI_PA0001.ENDDA") == "99991231"),
        "left"
    )

    .join(
        VBFA.alias("VBFA"),
        (F.col("VBFA.VBELV") == F.col("ZXVB1.VBELN")) &
        (F.col("VBFA.POSNV") == F.col("ZXVB1.POSNR")) &
        (F.col("VBFA.RN") == 1),
        "left"
    )

    .join(
        VBRK.alias("VBRK"),
        (F.col("VBRK.VBELN") == F.col("VBFA.INTCO_VBELN")) &
        (F.col("VBRK.RN") == 1),
        "left"
    )

    .join(
        vbkd_src.alias("VBKD"),
        (F.col("VBKD.VBELN") == F.col("ZXVB1.VBELN")) &
        (F.col("VBKD.TRATY") == ""),
        "left"
    )

    .join(
        tvtyt_src.alias("TVTYT"),
        F.col("TVTYT.TRATY") == F.col("VBKD.TRATY"),
        "left"
    )

    .select(
        trim_leading_zeros("ZXVB1.MATNR").alias("Material"),
        F.col("ZXVB1.SPART").alias("Product_group"),
        F.col("ZXVB1.WWLNE").alias("Product_Line"),
        F.col("ZXVB1.AUART").alias("Sales_Document_Type"),
        trim_leading_zeros("ZXVB1.VBELN").alias("Sales_Document"),
        trim_leading_zeros("ZXVB1.POSNR").alias("Item"),
        F.col("ZXVB1.VBQNT").cast("decimal(15,2)").alias("Order_quantity"),
        F.col("ZXVB1.VFQNT").cast("decimal(15,2)").alias("Billed_quantity"),
        trim_leading_zeros("ZXVB1.KUNNR").alias("Customer"),

        sap_date_nullable("ZXVB1.AUDAT").alias("Document_Date"),
        sap_date_nullable("ZXVB1.VDATU").alias("Requested_deliv_date"),
        sap_date_nullable("ZXVB1.EDATU").alias("Delivery_Date"),
        sap_date_nullable("ZXVB1.VFDAT").alias("Billing_Date"),

        F.col("ZXVB1.AUTLF").alias("Complete_delivery"),
        F.col("ZXVB1.BSTNK").alias("Customer_ref_no"),
        sap_date_nullable("ZXVB1.GIDAT").alias("Goods_Issue_Date"),

        F.when(F.col("ZXVB1.ICTYP") == "B", "Back-to-back")
         .when(F.col("ZXVB1.ICTYP") == "C", "Contracts")
         .when(F.col("ZXVB1.ICTYP") == "D", "Drop")
         .when(F.col("ZXVB1.ICTYP") == "G", "Credit")
         .when(F.col("ZXVB1.ICTYP") == "K", "Consignment")
         .when(F.col("ZXVB1.ICTYP") == "L", "Debit")
         .when(F.col("ZXVB1.ICTYP") == "N", "Non-stock")
         .when(F.col("ZXVB1.ICTYP") == "Q", "Quote")
         .when(F.col("ZXVB1.ICTYP") == "R", "Return")
         .when(F.col("ZXVB1.ICTYP") == "S", "Stock")
         .when(F.col("ZXVB1.ICTYP") == "Z", "Other")
         .otherwise("")
         .alias("Item_category"),

        F.col("ZXVB1.VBVAL").cast("decimal(15,2)").alias("Order_value"),
        F.col("ZXVB1.WERKS").alias("Plant"),
        sap_date_nullable("ZXVB1.GRDAT").alias("Posting_Date"),
        F.col("ZXVB1.LGORT").alias("Storage_Location"),
   
        F.col("MAKT.MAKTX").alias("Material_Description"),
        F.col("KNA1.NAME1").alias("Customer_name"),
        F.col("KNA1.ORT01").alias("Ship_to_city"),
        F.col("KNA1.PSTLZ").alias("Ship_to_post_zip_code"),
        F.col("KNA1.REGIO").alias("Ship_to_region_state"),
        F.col("KNA1.LAND1").alias("Ship_to_country"),
        F.col("KNA1.NAME1").alias("Ship_to_name"),

        F.col("MBEW.VERPR").cast("decimal(15,2)").alias("Moving_price"),
        F.col("PA0001.ENAME").alias("ZR_name"),
        F.col("PA0001.PERNR").alias("ZR_Rep"),
        F.col("ZI_PA0001.PERNR").alias("ZI_Rep"),
        F.col("ZI_PA0001.ENAME").alias("ZI_name"),

        F.col("VBFA.INTCO_VBELN").alias("IntCo_no"),
        sap_date_nullable("VBRK.FKDAT").alias("IntCo_date"),

        F.col("TVTYT.VTEXT").alias("Description"),
        F.col("ZXVB1.WAERK").alias("Doc_Currency"),
        F.concat(F.col("ZXVB1.VBELN"), F.col("ZXVB1.POSNR")).alias("INTEGRATION_ID")
    )
)

display(result)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save to Silver Lakehouse
result.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Silver_LH.analytics.sales_order_returnd_detail")

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
