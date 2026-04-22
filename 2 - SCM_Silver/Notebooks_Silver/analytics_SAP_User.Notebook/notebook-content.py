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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

usr02_df = spark.read.table("SCM_Bronze_LH.sap.usr02").filter(col("MANDT") == 100)
usr21_df = spark.read.table("SCM_Bronze_LH.sap.usr21").filter(col("MANDT") == 100)
adrp_df = spark.read.table("SCM_Bronze_LH.sap.adrp").filter(col("client") == 100)
adr6_df = spark.read.table("SCM_Bronze_LH.sap.adr6").filter(col("client") == 100)
adrc_df = spark.read.table("SCM_Bronze_LH.sap.adrc").filter(col("client") == 100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adr6_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(adrc_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
usr02_df = spark.read.table("SCM_Bronze_LH.sap.usr02")
usr02_df = usr02_df.filter(col("MANDT") == 100)
usr02_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adrp_df.count()
adrc_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sap_user_df = (
    usr02_df.alias("usr02")
    .join(
        usr21_df.alias("usr21"),
        col("usr02.BNAME") == col("usr21.BNAME"),
        "left"
    )
    .join(
        adrp_df.alias("adrp"),
        col("usr21.PERSNUMBER") == col("adrp.PERSNUMBER"),
        "left"
    )
    .join(
        adr6_df.alias("adr6"),
        col("usr21.PERSNUMBER") == col("adr6.PERSNUMBER"),
        "left"
    )
    .join(
        adrc_df.alias("adrc"),
        col("usr21.ADDRNUMBER") == col("adrc.ADDRNUMBER"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sap_user_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adr6_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

adrc_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__ = sap_user_df.select(
    col("usr02.MANDT").alias("Client"),
    col("usr02.BNAME").alias("User"),
    col("usr02.CLASS").alias("User group"),
    col("usr21.PERSNUMBER").alias("Person number"),
    col("usr21.ADDRNUMBER").alias("Address number"),
    col("adrp.NAME_FIRST").alias("First name"),
    col("adrp.NAME_LAST").alias("Last name"),
    col("adrc.CITY1").alias("City"),
    col("adrc.POST_CODE1").alias("Postal Code"),
    col("adrc.STREET").alias("Street"),
    col("adrc.COUNTRY").alias("Country"),
    col("adrc.REGION").alias("Region"),
    col("adr6.SMTP_ADDR").alias("E-Mail Address"),
    # col("usr02.isActive")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in final_df__.columns:
    clean_name = c.strip().replace(" ", "_")
    final_df__ = final_df__.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_temp= spark.sql("SELECT * FROM SCM_Silver_LH.analytics.sap_user")
display(df_temp)

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

final_df__ = final_df__.filter(col("User").isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__ = final_df__.na.drop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.SAP_User")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, when

final_df_.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in final_df_.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_ = final_df_.dropna(subset=["User", "E-Mail_Address"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__ = final_df__.dropna(subset=[
    "User",
    "E-Mail_Address",
    "First_name",
    "Last_name"
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df__.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sap_user_df = sap_user_df.filter(col("usr02.USTYP") == "A")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sap_user_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sap_user_df = sap_user_df.filter(col("usr02.UFLAG") == 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_date

sap_user_df = sap_user_df.filter(
    (col("usr02.GLTGV") <= current_date()) &
    (col("usr02.GLTGB") >= current_date())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
