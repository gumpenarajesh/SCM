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

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara_df = spark.read.table("SCM_Bronze_LH.sap.MARA").alias("mara")
makt_df = spark.read.table("SCM_Bronze_LH.sap.MAKT").alias("makt")
mch1_df = spark.read.table("SCM_Bronze_LH.sap.MCH1").alias("mch1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_desc_join = mara_df.join(
    makt_df,
    col("mara.MATNR") == col("makt.MATNR"),
    "left"
).filter(
    ~col("makt.MAKTX").like("%delete%")
).select(
    col("mara.MATNR").alias("MATNR"),
    col("makt.MAKTX").alias("Material_Text")
).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_material = Window.partitionBy("MATNR").orderBy("MATNR")

materialdescription_df = material_desc_join.withColumn(
    "rownum",
    row_number().over(window_material)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_window = Window.partitionBy("MATNR","CHARG").orderBy(col("ERSDA").desc())

material_batches_df = mch1_df.filter(
    col("LVORM") == ""
).withColumn(
    "RN",
    row_number().over(batch_window)
).filter(
    col("RN") == 1
).select(
    col("MATNR"),
    col("CHARG"),
    col("VFDAT"),
    col("VERAB")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = material_batches_df.alias("b").join(
    materialdescription_df.alias("d"),
    (col("b.MATNR") == col("d.MATNR")) &
    (col("d.rownum") == 1),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_batches_final = joined_df.select(

    regexp_replace(col("b.MATNR"), "^0+", "").alias("Material"),

    col("b.CHARG").alias("Batch"),

    col("d.Material_Text").alias("Material Text"),

    when(col("b.VFDAT") == 0, "1900-01-01")
    .otherwise(to_date(col("b.VFDAT").cast("string"), "yyyyMMdd"))
    .alias("SLED/BBD"),

    when(col("b.VERAB") == 0, "1900-01-01")
    .otherwise(to_date(col("b.VERAB").cast("string"), "yyyyMMdd"))
    .alias("Available from")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_batches_final.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(material_batches_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in material_batches_final.columns:
    clean_name = c.strip().replace(" ", "_")
    material_batches_final = material_batches_final.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

material_batches_final.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.analytics.material_batches")

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
