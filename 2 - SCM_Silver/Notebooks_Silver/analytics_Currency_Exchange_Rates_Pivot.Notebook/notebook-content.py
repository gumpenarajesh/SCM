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
from pyspark.sql.functions import col, lit, max as _max, substring

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df  = spark.table("SCM_Silver_LH.analytics.currency_exchange_rates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

currency_exchange_rates.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bw_p_source = df.select(
    substring(col("Fiscal_year|period"), 1, 4).alias("Fiscal_year"),
    col("Posting_period"),
    col("Currency_key"),
    col("BW_P")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bw_p_pivot = bw_p_source.groupBy("Fiscal_year", "Currency_key") \
    .pivot("Posting_period", ["001","002","003","004","005","006","007","008","009","010","011","012"]) \
    .max("BW_P")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit

bw_p_final = bw_p_pivot.withColumn("Rate", lit("BW_P")) \
    .select("Rate", "Fiscal_year", "Currency_key",
            "001","002","003","004","005","006","007","008","009","010","011","012")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p_source = df.select(
    substring(col("Fiscal_year|period"), 1, 4).alias("Fiscal_year"),
    col("Posting_period"),
    col("Currency_key"),
    col("Rate_P")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p_pivot = p_source.groupBy("Fiscal_year", "Currency_key") \
    .pivot("Posting_period", ["001","002","003","004","005","006","007","008","009","010","011","012"]) \
    .max("Rate_P")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

p_final = p_pivot.withColumn("Rate", lit("Rate (P)")) \
    .select("Rate", "Fiscal_year", "Currency_key",
            "001","002","003","004","005","006","007","008","009","010","011","012")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bw_p_grouped = bw_p_final.dropDuplicates([
    "Rate", "Fiscal_year", "Currency_key",
    "001","002","003","004","005","006","007","008","009","010","011","012"
])

p_grouped = p_final.dropDuplicates([
    "Rate", "Fiscal_year", "Currency_key",
    "001","002","003","004","005","006","007","008","009","010","011","012"
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = bw_p_grouped.unionByName(p_grouped)

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

# CELL ********************

final_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_filtered=final_df.dropna()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_filtered.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_filtered.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Silver_LH.analytics.currency_exchange_rates_pivot")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
