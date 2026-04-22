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
from pyspark.sql import Window

# =========================================================
# CONFIG
# =========================================================
SOURCE_DB = "SCM_Bronze_LH.sap"

# =========================================================
# HELPERS
# =========================================================
def pick_col(df, alias_name, col_name, default=None, cast_type=None):
    """
    Return alias.col if the source dataframe has the column,
    otherwise return a literal default.
    """
    if col_name in df.columns:
        c = F.col(f"{alias_name}.{col_name}")
    else:
        c = F.lit(default)
    return c.cast(cast_type) if cast_type else c


def first_not_null_per_group(df, partition_cols, order_cols, select_cols, filter_expr=None):
    """
    Generic helper to keep 1 row per key.
    """
    x = df
    if filter_expr is not None:
        x = x.filter(filter_expr)

    w = Window.partitionBy(*partition_cols).orderBy(*order_cols)
    return (
        x.select(*select_cols)
         .withColumn("RN", F.row_number().over(w))
         .filter(F.col("RN") == 1)
         .drop("RN")
    )


# =========================================================
# LOAD SOURCE TABLES
# =========================================================
vbak = spark.table(f"{SOURCE_DB}.vbak")
vbap = spark.table(f"{SOURCE_DB}.vbap")
vbep = spark.table(f"{SOURCE_DB}.vbep")
vbfa = spark.table(f"{SOURCE_DB}.vbfa")
vbpa = spark.table(f"{SOURCE_DB}.vbpa")
likp = spark.table(f"{SOURCE_DB}.likp")
vbrk = spark.table(f"{SOURCE_DB}.vbrk")

# =========================================================
# VBEP: latest schedule line per sales doc/item
# =========================================================
vbep_latest = (
    vbep
    .select(
        "VBELN", "POSNR", "ETENR",
        *[c for c in ["EDATU", "BMENG", "LIFSP"] if c in vbep.columns]
    )
)

vbep_w = Window.partitionBy("VBELN", "POSNR").orderBy(F.col("ETENR").desc())

vbep_latest = (
    vbep_latest
    .withColumn("RN", F.row_number().over(vbep_w))
    .filter(F.col("RN") == 1)
    .drop("RN")
)

# =========================================================
# VBPA: first personnel number at item/header level
# Used to generate PZPZR / PZPZI if possible
# =========================================================
vbpa_pernr = vbpa.filter(F.col("PERNR").isNotNull() & (F.col("PERNR") != ""))

vbpa_item = (
    vbpa_pernr
    .select("VBELN", "POSNR", "PERNR")
    .withColumn("POSNR_KEY", F.col("POSNR"))
)

vbpa_item_w = Window.partitionBy("VBELN", "POSNR_KEY").orderBy(F.col("PERNR"))

vbpa_item = (
    vbpa_item
    .withColumn("RN", F.row_number().over(vbpa_item_w))
    .filter(F.col("RN") == 1)
    .drop("RN")
    .withColumnRenamed("PERNR", "ITEM_PERNR")
    .drop("POSNR_KEY")
)

vbpa_header = (
    vbpa_pernr
    .filter(F.col("POSNR") == "000000")
    .select("VBELN", "PERNR")
)

vbpa_header_w = Window.partitionBy("VBELN").orderBy(F.col("PERNR"))

vbpa_header = (
    vbpa_header
    .withColumn("RN", F.row_number().over(vbpa_header_w))
    .filter(F.col("RN") == 1)
    .drop("RN")
    .withColumnRenamed("PERNR", "HDR_PERNR")
)

# =========================================================
# VBFA -> DELIVERY LINK
# VBTYP_N J/T usually delivery-related
# =========================================================
delivery_link = (
    vbfa
    .filter(F.col("VBTYP_N").isin("J", "T"))
    .select(
        "VBELV", "POSNV", "VBELN",
        *[c for c in ["ERDAT", "ERZET"] if c in vbfa.columns]
    )
)

delivery_w = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc_nulls_last() if "ERDAT" in delivery_link.columns else F.lit(1),
    F.col("ERZET").desc_nulls_last() if "ERZET" in delivery_link.columns else F.lit(1),
    F.col("VBELN").desc_nulls_last()
)

delivery_link = (
    delivery_link
    .withColumn("RN", F.row_number().over(delivery_w))
    .filter(F.col("RN") == 1)
    .drop("RN")
    .withColumnRenamed("VBELV", "DLV_VBELV")
    .withColumnRenamed("POSNV", "DLV_POSNV")
    .withColumnRenamed("VBELN", "DLV_VBELN")
    .withColumnRenamed("ERDAT", "DLV_ERDAT")
    .withColumnRenamed("ERZET", "DLV_ERZET")
)

# =========================================================
# VBFA -> BILLING LINK
# Broad billing set; if not present, fallback logic later will still work
# =========================================================
billing_link = (
    vbfa
    .filter(F.col("VBTYP_N").isin("M", "N", "O", "P", "R", "U", "V", "5", "6"))
    .select(
        "VBELV", "POSNV", "VBELN",
        *[c for c in ["ERDAT", "ERZET"] if c in vbfa.columns]
    )
)

billing_w = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc_nulls_last() if "ERDAT" in billing_link.columns else F.lit(1),
    F.col("ERZET").desc_nulls_last() if "ERZET" in billing_link.columns else F.lit(1),
    F.col("VBELN").desc_nulls_last()
)

billing_link = (
    billing_link
    .withColumn("RN", F.row_number().over(billing_w))
    .filter(F.col("RN") == 1)
    .drop("RN")
    .withColumnRenamed("VBELV", "BIL_VBELV")
    .withColumnRenamed("POSNV", "BIL_POSNV")
    .withColumnRenamed("VBELN", "BIL_VBELN")
    .withColumnRenamed("ERDAT", "BIL_ERDAT")
    .withColumnRenamed("ERZET", "BIL_ERZET")
)

# =========================================================
# LIKP minimal delivery dates
# =========================================================
likp_min = likp.select(
    "VBELN",
    *[c for c in ["WADAT", "LFDAT", "ERDAT"] if c in likp.columns]
)

# =========================================================
# VBRK minimal billing dates
# =========================================================
vbrk_min = vbrk.select(
    "VBELN",
    *[c for c in ["FKDAT", "ERDAT"] if c in vbrk.columns]
)

# =========================================================
# BASE JOIN
# =========================================================
base = (
    vbak.alias("VBAK")
    .join(
        vbap.alias("VBAP"),
        F.col("VBAK.VBELN") == F.col("VBAP.VBELN"),
        "inner"
    )
    .join(
        vbep_latest.alias("VBEP"),
        (F.col("VBAP.VBELN") == F.col("VBEP.VBELN")) &
        (F.col("VBAP.POSNR") == F.col("VBEP.POSNR")),
        "left"
    )
    .join(
        vbpa_item.alias("VBPA_ITEM"),
        (F.col("VBAP.VBELN") == F.col("VBPA_ITEM.VBELN")) &
        (F.col("VBAP.POSNR") == F.col("VBPA_ITEM.POSNR")),
        "left"
    )
    .join(
        vbpa_header.alias("VBPA_HDR"),
        F.col("VBAK.VBELN") == F.col("VBPA_HDR.VBELN"),
        "left"
    )
    .join(
        delivery_link.alias("DLV"),
        (F.col("VBAP.VBELN") == F.col("DLV.DLV_VBELV")) &
        (F.col("VBAP.POSNR") == F.col("DLV.DLV_POSNV")),
        "left"
    )
    .join(
        billing_link.alias("BIL"),
        (F.col("VBAP.VBELN") == F.col("BIL.BIL_VBELV")) &
        (F.col("VBAP.POSNR") == F.col("BIL.BIL_POSNV")),
        "left"
    )
    .join(
        likp_min.alias("LIKP"),
        F.col("DLV.DLV_VBELN") == F.col("LIKP.VBELN"),
        "left"
    )
    .join(
        vbrk_min.alias("VBRK"),
        F.col("BIL.BIL_VBELN") == F.col("VBRK.VBELN"),
        "left"
    )
)

# =========================================================
# BUILD BACKLOG
# Requested rule:
# - use real SAP column if present
# - if source is empty/null -> derive from related SAP data
# - if nothing available -> generate fallback
# =========================================================
backlog = base.select(
    # -----------------------------------------------------
    # Core columns
    # -----------------------------------------------------
    pick_col(vbak, "VBAK", "MANDT", "").alias("MANDT"),
    F.col("VBAP.VBELN").alias("VBELN"),
    F.col("VBAP.POSNR").alias("POSNR"),
    pick_col(vbak, "VBAK", "VKORG", "").alias("VKORG"),

    # -----------------------------------------------------
    # PSTAT generated
    # A = open, B = delivery exists, C = billing exists, X = rejected
    # -----------------------------------------------------
    F.when(F.col("VBAP.ABGRU").isNotNull() & (F.col("VBAP.ABGRU") != ""), F.lit("X"))
     .when(F.col("BIL.BIL_VBELN").isNotNull(), F.lit("C"))
     .when(F.col("DLV.DLV_VBELN").isNotNull(), F.lit("B"))
     .otherwise(F.lit("A"))
     .alias("PSTAT"),

    # -----------------------------------------------------
    # BKREL from billing relevance
    # -----------------------------------------------------
    F.coalesce(F.col("VBAP.FKREL"), F.lit("")).alias("BKREL"),

    pick_col(vbap, "VBAP", "UEPOS", "").alias("UEPOS"),

    # -----------------------------------------------------
    # VBGJR / VBPER generated from requested delivery date
    # -----------------------------------------------------
    F.when(F.col("VBAK.VDATU").isNotNull(), F.substring(F.col("VBAK.VDATU").cast("string"), 1, 4))
     .otherwise(F.lit(""))
     .alias("VBGJR"),

    F.when(F.col("VBAK.VDATU").isNotNull(), F.substring(F.col("VBAK.VDATU").cast("string"), 5, 2))
     .otherwise(F.lit(""))
     .alias("VBPER"),

    pick_col(vbak, "VBAK", "AUDAT", None).alias("AUDAT"),
    pick_col(vbak, "VBAK", "AUART", "").alias("AUART"),
    pick_col(vbak, "VBAK", "ERDAT", None).alias("ERDAT"),
    pick_col(vbak, "VBAK", "ERNAM", "").alias("ERNAM"),

    # -----------------------------------------------------
    # AUGRU direct from VBAK, else blank
    # -----------------------------------------------------
    F.coalesce(F.col("VBAK.AUGRU"), F.lit("")).alias("AUGRU"),

    pick_col(vbap, "VBAP", "ABGRU", "").alias("ABGRU"),
    pick_col(vbak, "VBAK", "BSTNK", "").alias("BSTNK"),

    # -----------------------------------------------------
    # PZPZR generated from item PERNR else header PERNR else blank
    # -----------------------------------------------------
    F.coalesce(F.col("VBPA_ITEM.ITEM_PERNR"), F.col("VBPA_HDR.HDR_PERNR"), F.lit("")).alias("PZPZR"),

    # -----------------------------------------------------
    # ICTYP generated
    # -----------------------------------------------------
    F.when(F.col("VBAK.AUART").like("R%"), F.lit("R"))
     .when(F.col("VBAP.PSTYV").like("REN%"), F.lit("R"))
     .when(F.col("VBAP.PSTYV").like("RE%"), F.lit("R"))
     .when(F.col("VBAP.FKREL").isNotNull() & (F.col("VBAP.FKREL") != ""), F.lit("C"))
     .otherwise(F.lit("S"))
     .alias("ICTYP"),

    pick_col(vbap, "VBAP", "PSTYV", "").alias("PSTYV"),

    # -----------------------------------------------------
    # AUTLF from VBAK, else blank
    # -----------------------------------------------------
    F.coalesce(F.col("VBAK.AUTLF"), F.lit("")).alias("AUTLF"),

    # -----------------------------------------------------
    # WWDIV generated from SPART
    # -----------------------------------------------------
    F.coalesce(F.col("VBAP.SPART"), F.lit("")).alias("WWDIV"),

    pick_col(vbak, "VBAK", "KUNNR", "").alias("KUNNR"),
    pick_col(vbap, "VBAP", "MATNR", "").alias("MATNR"),
    pick_col(vbap, "VBAP", "SPART", "").alias("SPART"),

    # -----------------------------------------------------
    # WWLNE generated from PRODH first 5 chars, else blank
    # -----------------------------------------------------
    F.when(F.col("VBAP.PRODH").isNotNull() & (F.col("VBAP.PRODH") != ""), F.substring(F.col("VBAP.PRODH"), 1, 5))
     .otherwise(F.lit(""))
     .alias("WWLNE"),

    pick_col(vbap, "VBAP", "WERKS", "").alias("WERKS"),
    pick_col(vbap, "VBAP", "LGORT", "").alias("LGORT"),

    # -----------------------------------------------------
    # PZPZI generated from header/item PERNR fallback
    # -----------------------------------------------------
    F.coalesce(F.col("VBPA_HDR.HDR_PERNR"), F.col("VBPA_ITEM.ITEM_PERNR"), F.lit("")).alias("PZPZI"),

    pick_col(vbap, "VBAP", "PAOBJNR", "").alias("PAOBJNR"),
    pick_col(vbak, "VBAK", "VDATU", None).alias("VDATU"),
    pick_col(vbep_latest, "VBEP", "EDATU", None).alias("EDATU"),

    # -----------------------------------------------------
    # CNFDT generated from schedule date else requested date
    # -----------------------------------------------------
    F.coalesce(F.col("VBEP.EDATU"), F.col("VBAK.VDATU")).alias("CNFDT"),

    # -----------------------------------------------------
    # PODAT generated from delivery date / billing created / schedule / requested
    # -----------------------------------------------------
    F.coalesce(
        F.col("LIKP.LFDAT"),
        F.col("VBRK.ERDAT"),
        F.col("VBEP.EDATU"),
        F.col("VBAK.VDATU")
    ).alias("PODAT"),

    # -----------------------------------------------------
    # GRDAT generated from delivery GI date / delivery date / schedule
    # -----------------------------------------------------
    F.coalesce(
        F.col("LIKP.WADAT"),
        F.col("LIKP.LFDAT"),
        F.col("VBEP.EDATU")
    ).alias("GRDAT"),

    # -----------------------------------------------------
    # IRDAT generated from invoice date / billing flow date
    # -----------------------------------------------------
    F.coalesce(
        F.col("VBRK.FKDAT"),
        F.col("BIL.BIL_ERDAT")
    ).alias("IRDAT"),

    # -----------------------------------------------------
    # VLDAT generated from delivery planned / schedule / requested
    # -----------------------------------------------------
    F.coalesce(
        F.col("LIKP.LFDAT"),
        F.col("VBEP.EDATU"),
        F.col("VBAK.VDATU")
    ).alias("VLDAT"),

    # -----------------------------------------------------
    # GIDAT generated from actual GI / delivery flow date
    # -----------------------------------------------------
    F.coalesce(
        F.col("LIKP.WADAT"),
        F.col("DLV.DLV_ERDAT")
    ).alias("GIDAT"),

    # -----------------------------------------------------
    # VFDAT generated from billing date / billing flow date
    # -----------------------------------------------------
    F.coalesce(
        F.col("VBRK.FKDAT"),
        F.col("BIL.BIL_ERDAT")
    ).alias("VFDAT"),

    F.coalesce(F.col("VBAP.AEDAT"), F.col("VBAK.AEDAT")).alias("AEDAT"),

    # -----------------------------------------------------
    # AEZET: if VBAP has ERZET use it, else VBAK.ERZET, else blank
    # -----------------------------------------------------
    (
        F.col("VBAP.ERZET") if "ERZET" in vbap.columns
        else (F.col("VBAK.ERZET") if "ERZET" in vbak.columns else F.lit(""))
    ).alias("AEZET"),

    # -----------------------------------------------------
    # AENAM: generated from creator if no change-user field exists
    # -----------------------------------------------------
    F.coalesce(F.col("VBAK.ERNAM"), F.lit("")).alias("AENAM"),

    pick_col(vbak, "VBAK", "WAERK", "").alias("WAERK"),
    F.lit(0).cast("decimal(15,2)").alias("ABSMG"),

    # -----------------------------------------------------
    # PERIO generated YYYYMM from VDATU
    # -----------------------------------------------------
    F.when(
        F.col("VBAK.VDATU").isNotNull(),
        F.concat(
            F.substring(F.col("VBAK.VDATU").cast("string"), 1, 4),
            F.substring(F.col("VBAK.VDATU").cast("string"), 5, 2)
        )
    ).otherwise(F.lit("")).alias("PERIO"),

    # -----------------------------------------------------
    # Quantities / values
    # -----------------------------------------------------
    pick_col(vbap, "VBAP", "KWMENG", 0, "decimal(15,2)").alias("VBQNT"),
    F.lit(0).cast("decimal(15,2)").alias("POQNT"),
    F.lit(0).cast("decimal(15,2)").alias("GRQNT"),
    F.lit(0).cast("decimal(15,2)").alias("IRQNT"),
    F.lit(0).cast("decimal(15,2)").alias("VLQNT"),
    F.lit(0).cast("decimal(15,2)").alias("GIQNT"),
    F.lit(0).cast("decimal(15,2)").alias("VFQNT"),
    pick_col(vbap, "VBAP", "NETWR", 0, "decimal(18,2)").alias("VBVAL"),
    F.lit(0).cast("decimal(18,2)").alias("VBDSC"),
    F.lit(0).cast("decimal(18,2)").alias("VBCST")

)
# Save to Silver Lakehouse
backlog.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Bronze_LH.sap.backlog")
# =========================================================
# VIEW RESULT
# =========================================================
display(backlog)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

backlog = spark.table("SCM_Bronze_LH.sap.backlog")

clean_generated = (
    backlog
    .select(
        F.col("VBELN"),
        F.col("POSNR"),

        F.when(
            F.col("PZPZR").isNotNull() &
            (F.col("PZPZR") != "") &
            (F.col("PZPZR") != "00000000"),
            F.col("PZPZR")
        ).otherwise(
            F.concat(F.lit("ZR"), F.col("VKORG"), F.col("VBELN"))
        ).alias("zr_rep"),

        F.when(
            F.col("PZPZR").isNotNull() &
            (F.col("PZPZR") != "") &
            (F.col("PZPZR") != "00000000"),
            F.col("PZPZR")
        ).otherwise(
            F.concat(F.lit("ZR"), F.col("VKORG"), F.col("VBELN"))
        ).alias("zr_name"),

        F.when(
            F.col("PZPZI").isNotNull() &
            (F.col("PZPZI") != "") &
            (F.col("PZPZI") != "00000000"),
            F.col("PZPZI")
        ).otherwise(
            F.concat(F.lit("ZI"), F.col("VKORG"), F.col("VBELN"), F.col("POSNR"))
        ).alias("zi_rep"),

        F.when(
            F.col("PZPZI").isNotNull() &
            (F.col("PZPZI") != "") &
            (F.col("PZPZI") != "00000000"),
            F.col("PZPZI")
        ).otherwise(
            F.concat(F.lit("ZI"), F.col("VKORG"), F.col("VBELN"), F.col("POSNR"))
        ).alias("zi_name"),

        F.concat(F.lit("IC"), F.col("VBELN"), F.col("POSNR")).alias("intco_no"),

        F.coalesce(
            F.col("IRDAT"),
            F.col("VFDAT"),
            F.col("GIDAT"),
            F.col("GRDAT"),
            F.col("PODAT"),
            F.col("CNFDT"),
            F.col("VDATU"),
            F.col("EDATU"),
            F.col("AUDAT"),
            F.col("ERDAT")
        ).alias("intco_date"),

        F.concat(F.lit("BIL"), F.col("VBELN"), F.col("POSNR")).alias("billing_document"),

        F.when(
            F.col("MATNR").isNotNull() & (F.col("MATNR") != ""),
            F.col("MATNR")
        ).otherwise(
            F.concat(F.col("VBELN"), F.col("POSNR"))
        ).alias("description")
    )
)

display(clean_generated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# =========================================================
# LOAD ORIGINAL TABLE
# =========================================================
backlog = spark.table("SCM_Bronze_LH.sap.backlog")

# =========================================================
# REPLACE ORIGINAL COLUMNS WITH GENERATED VALUES
# =========================================================
backlog_updated = (
    backlog
    .withColumn(
        "PZPZR",
        F.when(
            F.col("PZPZR").isNotNull() &
            (F.col("PZPZR") != "") &
            (F.col("PZPZR") != "00000000"),
            F.col("PZPZR")
        ).otherwise(
            F.concat(F.lit("ZR"), F.col("VKORG"), F.col("VBELN"))
        )
    )
    .withColumn(
        "PZPZI",
        F.when(
            F.col("PZPZI").isNotNull() &
            (F.col("PZPZI") != "") &
            (F.col("PZPZI") != "00000000"),
            F.col("PZPZI")
        ).otherwise(
            F.concat(F.lit("ZI"), F.col("VKORG"), F.col("VBELN"), F.col("POSNR"))
        )
    )
    .withColumn(
        "IRDAT",
        F.coalesce(
            F.col("IRDAT"),
            F.col("VFDAT"),
            F.col("GIDAT"),
            F.col("GRDAT"),
            F.col("PODAT"),
            F.col("CNFDT"),
            F.col("VDATU"),
            F.col("EDATU"),
            F.col("AUDAT"),
            F.col("ERDAT")
        )
    )
)

# =========================================================
# SAVE BACK INTO ORIGINAL TABLE
# =========================================================
backlog_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.sap.backlog")

# =========================================================
# VIEW
# =========================================================
display(backlog_updated.select("VBELN", "POSNR", "PZPZR", "PZPZI", "IRDAT"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_DB = "SCM_Bronze_LH.sap"

# =========================================================
# LOAD
# =========================================================
backlog = spark.table(f"{SOURCE_DB}.backlog")
pa0001 = spark.table(f"{SOURCE_DB}.pa0001")
vbfa = spark.table(f"{SOURCE_DB}.vbfa")
vbrk = spark.table(f"{SOURCE_DB}.vbrk")
vbkd = spark.table(f"{SOURCE_DB}.vbkd")

# =========================================================
# 1) PA0001
# Generate/fill native source columns: PERNR, ENAME
# from backlog PZPZR / PZPZI
# =========================================================
rep_seed = (
    backlog
    .select(
        F.col("PZPZR").alias("PERNR"),
        F.concat(F.lit("ZR"), F.col("VKORG"), F.col("VBELN")).alias("ENAME")
    )
    .unionByName(
        backlog.select(
            F.col("PZPZI").alias("PERNR"),
            F.concat(F.lit("ZI"), F.col("VKORG"), F.col("VBELN"), F.col("POSNR")).alias("ENAME")
        )
    )
    .filter(
        F.col("PERNR").isNotNull() &
        (F.col("PERNR") != "") &
        (F.col("PERNR") != "00000000")
    )
    .dropDuplicates(["PERNR"])
)

pa0001_updated = (
    pa0001.alias("P")
    .join(rep_seed.alias("R"), F.col("P.PERNR") == F.col("R.PERNR"), "full_outer")
    .select(
        F.coalesce(F.col("P.MANDT"), F.lit("100")).alias("MANDT"),
        F.coalesce(F.col("P.PERNR"), F.col("R.PERNR")).alias("PERNR"),
        F.coalesce(
            F.when(F.col("P.ENAME").isNull() | (F.col("P.ENAME") == ""), F.col("R.ENAME")),
            F.col("P.ENAME"),
            F.col("R.ENAME")
        ).alias("ENAME"),
        F.coalesce(F.col("P.ENDDA"), F.lit("99991231")).alias("ENDDA"),
        *[
            F.col(f"P.{c}")
            for c in pa0001.columns
            if c not in {"MANDT", "PERNR", "ENAME", "ENDDA"}
        ]
    )
)

pa0001_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SOURCE_DB}.pa0001")

# =========================================================
# 2) VBFA
# Generate/fill native source column: VBELN
# for IntCo / Billing from backlog keys
# =========================================================
vbfa_seed = (
    backlog
    .select(
        F.col("VBELN").alias("VBELV"),
        F.col("POSNR").alias("POSNV"),
        F.concat(F.lit("IC"), F.col("VBELN"), F.col("POSNR")).alias("GEN_INTCO_VBELN"),
        F.concat(F.lit("BIL"), F.col("VBELN"), F.col("POSNR")).alias("GEN_BILL_VBELN")
    )
)

vbfa_intco_existing = vbfa.filter(F.col("VBTYP_N") == "5").select("VBELV", "POSNV", "VBELN")
vbfa_bill_existing = vbfa.filter(F.col("VBTYP_N").isin("M", "N", "O", "P", "R", "U", "V")).select("VBELV", "POSNV", "VBELN")

vbfa_intco_new = (
    vbfa_seed.alias("S")
    .join(
        vbfa_intco_existing.alias("E"),
        (F.col("S.VBELV") == F.col("E.VBELV")) &
        (F.col("S.POSNV") == F.col("E.POSNV")),
        "left_anti"
    )
    .select(
        F.lit("100").alias("MANDT"),
        F.col("S.VBELV").alias("VBELV"),
        F.col("S.POSNV").alias("POSNV"),
        F.col("S.GEN_INTCO_VBELN").alias("VBELN"),
        F.lit("5").alias("VBTYP_N")
    )
)

vbfa_bill_new = (
    vbfa_seed.alias("S")
    .join(
        vbfa_bill_existing.alias("E"),
        (F.col("S.VBELV") == F.col("E.VBELV")) &
        (F.col("S.POSNV") == F.col("E.POSNV")),
        "left_anti"
    )
    .select(
        F.lit("100").alias("MANDT"),
        F.col("S.VBELV").alias("VBELV"),
        F.col("S.POSNV").alias("POSNV"),
        F.col("S.GEN_BILL_VBELN").alias("VBELN"),
        F.lit("M").alias("VBTYP_N")
    )
)

vbfa_existing_trim = vbfa.select(*[c for c in vbfa.columns if c in {"MANDT", "VBELV", "POSNV", "VBELN", "VBTYP_N"}])

vbfa_updated = (
    vbfa_existing_trim
    .unionByName(vbfa_intco_new.select(*vbfa_existing_trim.columns), allowMissingColumns=True)
    .unionByName(vbfa_bill_new.select(*vbfa_existing_trim.columns), allowMissingColumns=True)
    .dropDuplicates(["VBELV", "POSNV", "VBELN", "VBTYP_N"])
)

vbfa_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SOURCE_DB}.vbfa")

# =========================================================
# 3) VBRK
# Generate/fill native source column: FKDAT
# for generated INTCO VBELN
# =========================================================
vbrk_seed = (
    backlog
    .select(
        F.concat(F.lit("IC"), F.col("VBELN"), F.col("POSNR")).alias("VBELN"),
        F.coalesce(
            F.col("IRDAT"),
            F.col("VFDAT"),
            F.col("GIDAT"),
            F.col("GRDAT"),
            F.col("PODAT"),
            F.col("CNFDT"),
            F.col("VDATU"),
            F.col("EDATU"),
            F.col("AUDAT"),
            F.col("ERDAT")
        ).alias("FKDAT")
    )
    .filter(F.col("VBELN").isNotNull() & (F.col("VBELN") != ""))
)

vbrk_existing_keys = vbrk.select("VBELN").dropDuplicates()

vbrk_new = (
    vbrk_seed.alias("S")
    .join(vbrk_existing_keys.alias("E"), F.col("S.VBELN") == F.col("E.VBELN"), "left_anti")
    .select(
        F.lit("100").alias("MANDT"),
        F.col("S.VBELN").alias("VBELN"),
        F.col("S.FKDAT").alias("FKDAT")
    )
)

vbrk_existing_trim = vbrk.select(*[c for c in vbrk.columns if c in {"MANDT", "VBELN", "FKDAT"}])

vbrk_updated = (
    vbrk_existing_trim
    .unionByName(vbrk_new.select(*vbrk_existing_trim.columns), allowMissingColumns=True)
    .dropDuplicates(["VBELN"])
)

vbrk_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SOURCE_DB}.vbrk")

# =========================================================
# 4) VBKD
# Generate/fill native source column: TRATY
# from backlog business keys
# =========================================================
vbkd_seed = (
    backlog
    .select(
        F.col("VBELN"),
        F.when(
            F.col("MATNR").isNotNull() & (F.col("MATNR") != ""),
            F.col("MATNR")
        ).otherwise(
            F.concat(F.col("VBELN"), F.col("POSNR"))
        ).alias("GEN_TRATY")
    )
    .dropDuplicates(["VBELN"])
)

vbkd_updated = (
    vbkd.alias("K")
    .join(vbkd_seed.alias("S"), F.col("K.VBELN") == F.col("S.VBELN"), "full_outer")
    .select(
        F.coalesce(F.col("K.VBELN"), F.col("S.VBELN")).alias("VBELN"),
        F.coalesce(
            F.when(F.col("K.TRATY").isNull() | (F.col("K.TRATY") == ""), F.col("S.GEN_TRATY")),
            F.col("K.TRATY"),
            F.col("S.GEN_TRATY")
        ).alias("TRATY"),
        *[
            F.col(f"K.{c}")
            for c in vbkd.columns
            if c not in {"VBELN", "TRATY"}
        ]
    )
)

vbkd_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SOURCE_DB}.vbkd")

# =========================================================
# CHECK
# =========================================================
display(
    spark.sql(f"""
        SELECT 'pa0001' AS tbl, COUNT(*) AS cnt FROM {SOURCE_DB}.pa0001
        UNION ALL
        SELECT 'vbfa', COUNT(*) FROM {SOURCE_DB}.vbfa
        UNION ALL
        SELECT 'vbrk', COUNT(*) FROM {SOURCE_DB}.vbrk
        UNION ALL
        SELECT 'vbkd', COUNT(*) FROM {SOURCE_DB}.vbkd
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# =========================================================
# LOAD ACTUAL TABLES
# =========================================================
backlog = spark.table("SCM_Bronze_LH.sap.backlog")
pa0001  = spark.table("SCM_Bronze_LH.sap.pa0001")
vbfa    = spark.table("SCM_Bronze_LH.sap.vbfa")
vbrk    = spark.table("SCM_Bronze_LH.sap.vbrk")
vbkd    = spark.table("SCM_Bronze_LH.sap.vbkd")

# =========================================================
# 1) BUILD pa0001_updated
# fill native columns: PERNR, ENAME, ENDDA
# =========================================================
pa_seed = (
    backlog
    .select(
        F.col("PZPZR").alias("PERNR"),
        F.col("PZPZR").alias("ENAME")
    )
    .unionByName(
        backlog.select(
            F.col("PZPZI").alias("PERNR"),
            F.col("PZPZI").alias("ENAME")
        )
    )
    .filter(
        F.col("PERNR").isNotNull() &
        (F.col("PERNR") != "") &
        (F.col("PERNR") != "00000000")
    )
    .dropDuplicates(["PERNR"])
)

pa_cols_other = [c for c in pa0001.columns if c not in {"MANDT", "PERNR", "ENAME", "ENDDA"}]

pa0001_updated = (
    pa0001.alias("P")
    .join(pa_seed.alias("S"), F.col("P.PERNR") == F.col("S.PERNR"), "full_outer")
    .select(
        F.coalesce(F.col("P.MANDT"), F.lit("100")).alias("MANDT"),
        F.coalesce(F.col("P.PERNR"), F.col("S.PERNR")).alias("PERNR"),
        F.coalesce(
            F.when(F.col("P.ENAME").isNull() | (F.col("P.ENAME") == ""), F.col("S.ENAME")),
            F.col("P.ENAME"),
            F.col("S.ENAME")
        ).alias("ENAME"),
        F.coalesce(F.col("P.ENDDA"), F.lit("99991231")).alias("ENDDA"),
        *[F.col(f"P.{c}").alias(c) for c in pa_cols_other]
    )
)

# =========================================================
# 2) BUILD vbfa_updated
# fill native columns: VBELV, POSNV, VBELN, VBTYP_N
# create generated IntCo and Billing rows
# =========================================================
vbfa_seed = (
    backlog
    .select(
        F.col("VBELN").alias("VBELV"),
        F.col("POSNR").alias("POSNV"),
        F.concat(F.lit("IC"), F.col("VBELN"), F.col("POSNR")).alias("INTCO_VBELN"),
        F.concat(F.lit("BIL"), F.col("VBELN"), F.col("POSNR")).alias("BILL_VBELN")
    )
)

vbfa_base_cols = [c for c in ["MANDT", "VBELV", "POSNV", "VBELN", "VBTYP_N"] if c in vbfa.columns]
vbfa_existing_trim = vbfa.select(*vbfa_base_cols)

vbfa_intco_new = (
    vbfa_seed
    .select(
        F.lit("100").alias("MANDT"),
        F.col("VBELV"),
        F.col("POSNV"),
        F.col("INTCO_VBELN").alias("VBELN"),
        F.lit("5").alias("VBTYP_N")
    )
)

vbfa_bill_new = (
    vbfa_seed
    .select(
        F.lit("100").alias("MANDT"),
        F.col("VBELV"),
        F.col("POSNV"),
        F.col("BILL_VBELN").alias("VBELN"),
        F.lit("M").alias("VBTYP_N")
    )
)

vbfa_new_trim = (
    vbfa_intco_new.select(*vbfa_base_cols)
    .unionByName(vbfa_bill_new.select(*vbfa_base_cols))
)

vbfa_updated = (
    vbfa_existing_trim
    .unionByName(vbfa_new_trim)
    .dropDuplicates(["VBELV", "POSNV", "VBELN", "VBTYP_N"])
)

# =========================================================
# 3) BUILD vbrk_updated
# fill native columns: VBELN, FKDAT
# =========================================================
vbrk_seed = (
    backlog
    .select(
        F.concat(F.lit("IC"), F.col("VBELN"), F.col("POSNR")).alias("VBELN"),
        F.coalesce(
            F.col("IRDAT"),
            F.col("VFDAT"),
            F.col("GIDAT"),
            F.col("GRDAT"),
            F.col("PODAT"),
            F.col("CNFDT"),
            F.col("VDATU"),
            F.col("EDATU"),
            F.col("AUDAT"),
            F.col("ERDAT")
        ).alias("FKDAT")
    )
    .filter(F.col("VBELN").isNotNull() & (F.col("VBELN") != ""))
)

vbrk_base_cols = [c for c in ["MANDT", "VBELN", "FKDAT"] if c in vbrk.columns]
vbrk_existing_trim = vbrk.select(*vbrk_base_cols)

vbrk_new_trim = (
    vbrk_seed
    .select(
        F.lit("100").alias("MANDT"),
        F.col("VBELN"),
        F.col("FKDAT")
    )
    .select(*vbrk_base_cols)
)

vbrk_updated = (
    vbrk_existing_trim
    .unionByName(vbrk_new_trim)
    .dropDuplicates(["VBELN"])
)

# =========================================================
# 4) BUILD vbkd_updated
# fill native columns: VBELN, TRATY
# =========================================================
vbkd_seed = (
    backlog
    .select(
        F.col("VBELN"),
        F.when(
            F.col("MATNR").isNotNull() & (F.col("MATNR") != ""),
            F.col("MATNR")
        ).otherwise(
            F.concat(F.col("VBELN"), F.col("POSNR"))
        ).alias("TRATY")
    )
    .dropDuplicates(["VBELN"])
)

vbkd_cols_other = [c for c in vbkd.columns if c not in {"VBELN", "TRATY"}]

vbkd_updated = (
    vbkd.alias("K")
    .join(vbkd_seed.alias("S"), F.col("K.VBELN") == F.col("S.VBELN"), "full_outer")
    .select(
        F.coalesce(F.col("K.VBELN"), F.col("S.VBELN")).alias("VBELN"),
        F.coalesce(
            F.when(F.col("K.TRATY").isNull() | (F.col("K.TRATY") == ""), F.col("S.TRATY")),
            F.col("K.TRATY"),
            F.col("S.TRATY")
        ).alias("TRATY"),
        *[F.col(f"K.{c}").alias(c) for c in vbkd_cols_other]
    )
)

# =========================================================
# CHECK
# =========================================================
display(pa0001_updated)
display(vbfa_updated)
display(vbrk_updated)
display(vbkd_updated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pa0001_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.sap.pa0001")

vbfa_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.sap.vbfa")

vbrk_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.sap.vbrk")

vbkd_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.sap.vbkd")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(backlog.select("VBELN", "POSNR", "PZPZR", "PZPZI", "IRDAT").limit(20))
display(pa0001.select("PERNR", "ENAME", "ENDDA").limit(20))
display(vbfa.select("VBELV", "POSNV", "VBELN", "VBTYP_N").limit(20))
display(vbrk.select("VBELN", "FKDAT").limit(20))
display(vbkd.select("VBELN", "TRATY").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
