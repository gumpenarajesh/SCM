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

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tvko_df = spark.read.table("SCM_Bronze_LH.sap.TVKO")   
t001_df = spark.read.table("SCM_Bronze_LH.sap.T001")   
tka01_df = spark.read.table("SCM_Bronze_LH.sap.TKA01") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tvko_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tvko_filtered = tvko_df.filter(
#     col("VKORG").like("%3")
# )

result = tvko_df.alias("tvko") \
    .join(t001_df.alias("t001"), col("tvko.BUKRS") == col("t001.BUKRS"), "left") \
    .join(tka01_df.alias("tka01"), col("tka01.KOKRS") == col("tvko.BUKRS"), "left") \
    .select(
        col("tvko.VKORG"),
        col("tvko.BUKRS"),
        col("t001.BUTXT"),
        col("tka01.BEZEI"),
        col("tka01.KTOPL")
    )


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

sales_org_joined = tvko_df.alias("tvko") \
.join(
    t001_df.alias("t001"),
    col("tvko.BUKRS") == col("t001.BUKRS"),
    "left"
) \
.join(
    tka01_df.alias("tka01"),
    col("tka01.KOKRS") == col("tvko.BUKRS"),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_org_joined.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_org_df = sales_org_joined.select(

    col("tvko.VKORG").alias("Sales Org"),
    col("tvko.BUKRS").alias("Company code"),

    col("t001.BUTXT").alias("Company Code Text"),

    col("tvko.WAERS").alias("Stats Currency"),

    col("tka01.BEZEI").alias("Name"),

    col("tvko.ADRNR").alias("Address"),

    col("t001.ORT01").alias("City"),
    col("t001.LAND1").alias("Country"),
    col("t001.SPRAS").alias("Language"),

    col("tka01.KTOPL").alias("Chart Of Accts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_org_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in sales_org_df.columns:
    clean_name = c.strip().replace(" ", "_")
    sales_org_df = sales_org_df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_org_df.write.mode("overwrite").saveAsTable("SCM_Silver_LH.analytics.Sales_Organization") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.Sales_Organization LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tvko_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
