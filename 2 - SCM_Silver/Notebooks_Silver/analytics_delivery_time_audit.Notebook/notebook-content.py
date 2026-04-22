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

from  pyspark.sql import functions as F
from  pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

makt_window = Window.partitionBy("MATNR").orderBy("MATNR")

makt_df = (
    spark.table("SCM_Bronze_LH.sap.MAKT")
    .filter(~F.col("MAKTX").like("%delete%"))
    .withColumn("rn", F.row_number().over(makt_window))
    .filter(F.col("rn") == 1)
    .select(
        "MATNR",
        F.col("MAKTX").alias("Material_Text")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_window = Window.partitionBy("VBELV", "POSNV") \
                    .orderBy(F.col("POSNV").desc())

vbfa_df = (
    spark.table("SCM_Bronze_LH.sap.VBFA")
    .filter(F.col("VBTYP_N") == "J")
    .withColumn("RN", F.row_number().over(vbfa_window))
    .filter(F.col("RN") == 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa1_window = Window.partitionBy("VBELV", "POSNV") \
                     .orderBy(
                         F.concat(F.col("ERDAT"), F.col("ERZET")).desc()
                     )

vbfa1_df = (
    spark.table("SCM_Bronze_LH.sap.VBFA")
    .filter(F.col("VBTYP_N") == "R")
    .withColumn("RN", F.row_number().over(vbfa1_window))
    .filter(F.col("RN") == 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nast_df = (
    spark.table("SCM_Bronze_LH.sap.NAST")
    .filter(
        (F.col("KAPPL") == "V2") &
        (F.col("KSCHL") == "ZD01")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbak = spark.table("SCM_Bronze_LH.sap.VBAK") \
    .filter(F.col("MANDT") == "100")

vbap = spark.table("SCM_Bronze_LH.sap.VBAP") \
    .filter(F.col("MANDT") == "100")
kna1 = spark.table("SCM_Bronze_LH.sap.KNA1").filter(F.col("MANDT")=="100")
vbpa = spark.table("SCM_Bronze_LH.sap.VBPA")
likp = spark.table("SCM_Bronze_LH.sap.LIKP")
mkpf = spark.table("SCM_Bronze_LH.sap.MKPF")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    vbak.alias("VBAK")

    # Customer
    .join(kna1.alias("KNA1"),
          F.col("VBAK.KUNNR") == F.col("KNA1.KUNNR"),
          "left")

    # Sales Item
    .join(vbap.alias("VBAP"),
          F.col("VBAK.VBELN") == F.col("VBAP.VBELN"),
          "left")

    # Material Text
    .join(makt_df.alias("MAKT"),
          F.col("VBAP.MATNR") == F.col("MAKT.MATNR"),
          "left")

    # VBFA (J type)
    .join(vbfa_df.alias("VBFA"),
          (F.col("VBFA.VBELV") == F.col("VBAP.VBELN")) &
          (F.col("VBFA.POSNV") == F.col("VBAP.POSNR")),
          "left")

    # VBFA (R type)
    .join(vbfa1_df.alias("VBFA1"),
          (F.col("VBFA1.VBELV") == F.col("VBAP.VBELN")) &
          (F.col("VBFA1.POSNV") == F.col("VBAP.POSNR")),
          "left")

    # Partner
    .join(vbpa.alias("VBPA"),
          F.col("VBPA.VBELN") == F.col("VBAK.VBELN"),
          "left")

    # Delivery
    .join(likp.alias("LIKP"),
          F.col("VBFA.VBELN") == F.col("LIKP.VBELN"),
          "left")

    # Output Type
    .join(nast_df.alias("NAST"),
          F.col("VBFA.VBELN") == F.col("NAST.OBJKY"),
          "left")

    # Material Document
    .join(mkpf.alias("MKPF"),
          F.col("VBFA1.VBELN") == F.col("MKPF.MBLNR"),
          "left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def trim_leading_zeros(col):
    return F.regexp_replace(col, r"^0+", "")

mkpf_ts = F.to_timestamp(
    F.concat_ws(
        " ",
        F.col("MKPF.BUDAT").cast("string"),
        F.format_string("%06d", F.col("MKPF.CPUTM").cast("int"))
    ),
    "yyyyMMdd HHmmss"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

posting_date = F.to_date(
    F.from_utc_timestamp(mkpf_ts, "EST")
)

final_df = df.withColumn("POSTING_DATE", posting_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(

    trim_leading_zeros(F.col("VBAK.VBELN")).alias("SalesDocument"),

    F.col("KNA1.NAME1").alias("CustomerName"),

    F.col("Material_Text").alias("Material_Description"),

    trim_leading_zeros(F.col("VBAP.MATNR")).alias("Material"),

    F.col("VBAP.SPART").alias("Product_Group"),

    
    trim_leading_zeros(F.col("VBAP.POSNR")).alias("Item"),

    F.col("VBAP.KWMENG").cast("decimal(15,2)").alias("Ordered_Qty"),

    F.when(F.col("VBAK.ERDAT") == 0, F.lit("1900-01-01"))
     .otherwise(F.to_date(F.col("VBAK.ERDAT").cast("string"), "yyyyMMdd"))
     .alias("Created_On"),

    trim_leading_zeros(F.col("VBFA.VBELN")).alias("Delivery_Number"),

    trim_leading_zeros(F.col("VBPA.KUNNR")).alias("Sold-to_Party"),


    F.col("VBAP.WERKS").alias("Plant"),

    F.when(
        F.col("NAST.ERDAT").isNull(),
        F.when(F.col("LIKP.ERDAT") == 0, F.lit("1900-01-01"))
         .otherwise(F.to_date(F.col("LIKP.ERDAT").cast("string"), "yyyyMMdd"))
    ).otherwise(
        F.when(F.col("NAST.ERDAT") == 0, F.lit("1900-01-01"))
         .otherwise(F.to_date(F.col("NAST.ERDAT").cast("string"), "yyyyMMdd"))
    ).alias("Del_Date"),

    posting_date.alias("Posting_Date"),

    F.when(
        (F.col("VBAP.ERDAT") == F.col("LIKP.ERDAT")) &
        (F.col("LIKP.ERZET") <= "160000"),
        F.lit("Y")
    ).when(
        F.col("NAST.ERDAT").isNotNull(),
        F.lit("N")
    ).otherwise(F.lit("X")).alias("PickOnTime"),


    F.datediff(
        posting_date,
        F.to_date(F.col("VBAK.ERDAT").cast("string"), "yyyyMMdd")
    ).alias("GI_Days"),

    F.col("VBAK.VKORG").alias("Sales_Org"),

    F.concat(
        F.col("VBAK.VBELN"),
        F.col("VBAP.POSNR")
    ).alias("INTEGRATION_ID")
).filter(
    F.col("SalesDocument").isNotNull() & F.col("Item").isNotNull()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Silver_LH.analytics.delivery_time_audit")

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
