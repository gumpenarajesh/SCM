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

from pyspark.sql.functions import col, regexp_replace

# Load source tables from Bronze Lakehouse
vbak = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.VBAK")
vbpa = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.VBPA")
adr6 = spark.sql("SELECT * FROM SCM_Bronze_LH.sap.adr6")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = vbak.alias("VBAK") \
    .join(
        vbpa.alias("VBPA"),
        col("VBAK.VBELN") == col("VBPA.VBELN"),
        "left"
    ) \
    .join(
        adr6.alias("ADR6"),
        (col("VBPA.ADRNR") == col("ADR6.ADDRNUMBER")) &
        (col("VBPA.ADRNP") == col("ADR6.PERSNUMBER")),
        "left"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, regexp_replace, lit

# Get VBAK columns
vbak_cols = vbak.columns

def safe_col(column_name):
    if column_name in vbak_cols:
        return col(f"VBAK.{column_name}")
    else:
        return lit("")

from pyspark.sql.functions import lpad

final_df = df.select(
    lpad(col("VBAK.VBELN"), 10, "0").alias("Sales_Document"),
    col("VBAK.VKBUR").alias("Sales_office"),
    lpad(col("VBAK.AUFNR"), 12, "0").alias("Order"),  # AUFNR is usually 12
    col("VBAK.BNAME").alias("Name"),
    col("ADR6.SMTP_ADDR").alias("Email_Address")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display result
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Save to Silver Lakehouse
final_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.analytics.sales_document_header_data")

# Display result
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
