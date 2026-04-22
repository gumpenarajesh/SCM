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

# Load and filter dependency tables
df_konh = spark.table("sap.konh").filter(F.col("MANDT") == "100")
df_konp = spark.table("sap.konp").filter(F.col("MANDT") == "100")
df_vbak = spark.table("sap.vbak").filter(F.col("MANDT") == "100")
df_vbap = spark.table("sap.vbap").filter(F.col("MANDT") == "100")

# Join logic
df_a994 = (
    df_konh.alias("h")
    .join(
        df_konp.alias("p"),
        F.col("h.KNUMH") == F.col("p.KNUMH"),
        "left"
    )
    .join(
        df_vbak.alias("vk"),
        F.col("h.MANDT") == F.col("vk.MANDT"),
        "left"
    )
    .join(
        df_vbap.alias("vp"),
        F.col("vk.VBELN") == F.col("vp.VBELN"),
        "left"
    )
    .select(
        F.col("h.MANDT").alias("MANDT"),
        F.col("h.KAPPL").alias("KAPPL"),
        F.col("h.KSCHL").alias("KSCHL"),
        F.col("vk.VKORG").alias("VKORG"),
        F.col("vp.MATNR").alias("ZZKITMAT"),
        F.col("vp.MATNR").alias("MATNR"),
        F.col("p.KFRST").alias("KFRST"),
        F.col("h.DATBI").alias("DATBI"),
        F.col("h.DATAB").alias("DATAB"),
        F.lit(None).cast("string").alias("KBSTAT"),
        F.col("h.KNUMH").alias("KNUMH")
    )
)

display(df_a994)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save dataframe to the SCM_Bronze_LH lakehouse Files area as CSV
df_a994.write.mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://f6eefe30-09e0-42fd-b87d-b4b372c2de7a@onelake.dfs.fabric.microsoft.com/f8eb1159-afd1-49f6-afb6-7d3267c1ab39/Files/a994_export")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save to Silver Lakehouse
df_a994.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Bronze_LH.sap.a994")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Create schema if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS sap")

# Load source table
vbpa = spark.table("sap.vbpa")

# Build sap.vbpa_parvw_zi
vbpa_parvw_zi = (
    vbpa
    .filter(F.col("PARVW") == "ZI")
    .select(
        F.col("VBELN").alias("VBELN"),
        F.col("POSNR").alias("POSNR"),
        F.col("PARVW").alias("PARVW"),
        F.col("KUNNR").alias("KUNNR"),
        F.col("LIFNR").alias("LIFNR"),
        F.col("PERNR").alias("PERNR"),
        F.col("PARNR").alias("PARNR"),
        F.col("ADRNR").alias("ADRNR"),
        F.col("ADRNP").alias("ADRNP"),
        F.col("HITYP").alias("HITYP"),
        F.col("HISTUNR").alias("HISTUNR")
    )
)

display(vbpa_parvw_zi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save to Silver Lakehouse
vbpa_parvw_zi.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Bronze_LH.sap.vbpa_parvw_zi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
