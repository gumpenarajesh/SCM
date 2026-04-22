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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F

# Read source tables
tvstt_df = spark.table("SCM_Bronze_LH.sap.tvstt").filter("MANDT='100'").filter("SPRAS='E'")
tvst_df = spark.table("SCM_Bronze_LH.sap.tvst").filter("MANDT='100'").filter("SPRAS='E'")
address_df = spark.table("SCM_Silver_LH.analytics.address")





# Select final columns


# Create view equivalent


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# First join TVSTT with TVST
df_join1 = tvstt_df.alias("TVSTT").join(
    tvst_df.alias("TVST"),
    F.col("TVST.VSTEL") == F.col("TVSTT.VSTEL"),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Second join with Address
df_join2 = df_join1.join(
    address_df.alias("Address"),
    F.col("Address.`Address_number`") == F.col("TVST.ADRNR"),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_address_shipping_point = df_join2.select(
    F.col("TVSTT.VSTEL").alias("Shipping Point"),
    F.col("TVSTT.VTEXT").alias("Shipping Point Text"),
    *[F.col(f"Address.`{c}`") for c in address_df.columns]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_address_shipping_point.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in df_address_shipping_point.columns:
    clean_name = c.strip().replace(" ", "_")
    df_address_shipping_point = df_address_shipping_point.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_address_shipping_point.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.address_shipping_point")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.address_shipping_point LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
