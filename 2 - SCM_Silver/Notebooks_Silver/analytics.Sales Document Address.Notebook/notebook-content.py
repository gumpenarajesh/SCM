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
# META     }
# META   }
# META }

# CELL ********************

##CREAT TABLE VBPA_SOLD_TO
vbpa = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.VBPA")

vbpa_sold_to = vbpa.filter("PARVW = 'AG'") \
.select(
    "VBELN",
    "POSNR",
    "PARVW",
    "KUNNR",
    "LIFNR",
    "PERNR",
    "PARNR",
    "ADRNR"
)

vbpa_sold_to.write.mode("overwrite").saveAsTable("SCM_Bronze_LH.sap.VBPA_SOLD_TO")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#CREAT TABLE VBPA_SHIP_TO
vbpa_ship_to = vbpa.filter("PARVW = 'WE'") \
.select(
    "VBELN",
    "POSNR",
    "PARVW",
    "KUNNR",
    "LIFNR",
    "PERNR",
    "PARNR",
    "ADRNR"
)

vbpa_ship_to.write.mode("overwrite").saveAsTable("SCM_Bronze_LH.sap.VBPA_SHIP_TO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit

# Load VBPA from Bronze
vbpa = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.VBPA")

# Sold-To (PARVW = AG)
sold = vbpa.filter(col("PARVW") == "AG").select(
    col("VBELN").alias("Sales Document"),
    col("ADRNR").alias("Address Sold To"),
    lit("").alias("Address Ship To")
)

# Ship-To (PARVW = WE)
ship = vbpa.filter(col("PARVW") == "WE").select(
    col("VBELN").alias("Sales Document"),
    lit("").alias("Address Sold To"),
    col("ADRNR").alias("Address Ship To")
)

# Join logic (same as SQL view)
result_df = ship.alias("ship").join(
    sold.alias("sold"),
    col("ship.Sales Document") == col("sold.Sales Document"),
    "left"
).select(
    col("ship.Sales Document"),
    col("sold.Address Sold To"),
    col("ship.Address Ship To")
)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

result_df_fixed = result_df.select(
    col("Sales Document").alias("Sales_Document"),
    col("Address Sold To").alias("Address_Sold_To"),
    col("Address Ship To").alias("Address_Ship_To")
)

result_df_fixed.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.analytics.Sales_Document_Address")



# Display result
#display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
