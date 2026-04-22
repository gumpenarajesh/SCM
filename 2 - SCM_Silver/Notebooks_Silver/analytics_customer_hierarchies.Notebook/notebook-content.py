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

from pyspark.sql.functions import col, regexp_replace, when, to_date, concat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.customer_hierarchies LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh = spark.table("sap.KNVH")
thitt = spark.table("sap.THITT")
customer = spark.table("SCM_Silver_LH.analytics.Customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed = knvh.withColumn(
    "Customer",
    regexp_replace(col("KUNNR"), "^0+", "")
).withColumn(
    "HglvCust",
    regexp_replace(col("HKUNNR"), "^0+", "")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed = knvh_transformed.withColumn(
    "Valid from",
    when(col("DATAB") == 0, "1900-01-01")
    .otherwise(to_date(col("DATAB").cast("string"), "yyyyMMdd"))
)

knvh_transformed = knvh_transformed.withColumn(
    "Valid to",
    when(col("DATBI") == 0, "1900-01-01")
    .otherwise(to_date(col("DATBI").cast("string"), "yyyyMMdd"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed = knvh_transformed.withColumn(
    "INTEGRATION_ID",
    concat(
        col("HITYP"),
        col("KUNNR"),
        col("VKORG"),
        col("VTWEG"),
        col("SPART"),
        col("DATAB")
    )
)
knvh_transformed.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(knvh_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvh_transformed.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = knvh_transformed.join(
    customer,
    customer["Customer"] == knvh_transformed["Customer"],
    "inner"
)
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.join(
    thitt,
    thitt["HITYP"] == knvh_transformed["HITYP"],
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter condition
df = df.filter(col("DATBI") >= "20190101")

# Final select
final_df = df.select(
    knvh_transformed["HITYP"].alias("Hierarchy type"),
    thitt["VTEXT"].alias("Hierarchy type Text"),
    knvh_transformed["Customer"],
    customer["Name"],
    knvh_transformed["VKORG"].alias("Sales Org."),
    knvh_transformed["VTWEG"].alias("Distr. Channel"),
    knvh_transformed["SPART"].alias("Product group"),
    col("Valid from"),
    col("Valid to"),
    knvh_transformed["HglvCust"],
    knvh_transformed["HVKORG"].alias("Hglv sales org."),
    knvh_transformed["HSPART"].alias("Hglv ProdGroup"),
    knvh_transformed["BOKRE"].alias("Rebate"),
    knvh_transformed["PRFRE"].alias("Price determin."),
    col("INTEGRATION_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.show()

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

final_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.analytics.customer_hierarchies")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from sap.thitt

# METADATA ********************

# META {
# META   "language": "sparksql",
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
