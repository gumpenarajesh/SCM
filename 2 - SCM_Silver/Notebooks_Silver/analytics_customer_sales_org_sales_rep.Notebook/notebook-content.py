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

from pyspark.sql.functions import col, substring, expr, length, lit,regexp_instr,concat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvp_df = spark.table("sap.KNVP").filter("MANDT='100'").alias("k")
pa0001_df = spark.table("sap.PA0001").filter("MANDT='100'").alias("p")
knvp_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = knvp_df.join(
    pa0001_df.filter(col("ENDDA") == "99991231"),
    knvp_df["PERNR"] == pa0001_df["PERNR"],
    "left"
)
joined_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# filtered_df = joined_df.filter(
#     (knvp_df["VTWEG"] == "XX") &
#     (knvp_df["SPART"] == "XX") &
#     (knvp_df["PARVW"] == "ZR")
# )
# filtered_df.show()
# filtered_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr

customer_col = expr("""
substring(
    KUNNR,
    regexp_instr(concat(KUNNR, '.'), '[^0]'),
    length(KUNNR)
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = joined_df.select(
    col("k.MANDT").alias("Client"),
    customer_col.alias("Customer"),
    col("k.VKORG").alias("Sales Org."),
    col("k.PARVW").alias("Partner Functn"),
    col("k.PERNR").alias("Sales rep for ORDER"),
    col("p.ENAME").alias("Sales rep for ORDER Text")
)

final_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.filter(
    col("Sales rep for ORDER").isNotNull()&
    col("Sales rep for ORDER Text").isNotNull()
)
  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in final_df.columns:
    clean_name = c.strip().replace(" ", "_")
    final_df = final_df.withColumnRenamed(c, clean_name)

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

# CELL ********************

len(final_df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write.mode("overwrite").saveAsTable("SCM_Silver_LH.analytics.customer_sales_org_sales_rep") 

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

pa0001_active = pa0001_df.filter(col("ENDDA") == "99991231")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pa0001_active.groupBy("PERNR").count().filter(col("count") > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pa0001_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(knvp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
