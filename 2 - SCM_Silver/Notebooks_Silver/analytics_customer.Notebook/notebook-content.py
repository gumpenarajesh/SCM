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

# kna1_df = spark.read.table("sap.kna1")
# t016t_df = spark.read.table("sap.T016T")
# tvk1t_df  = spark.read.table("sap.TVK1t")
# tbrct_df = spark.read.table("sap.TBRCT")
# t077x_df = spark.read.table("sap.T077X")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:sqlserver://sql-training-dhstech.database.windows.net:1433;databaseName=training") \
#     .option("dbtable", "(SELECT * FROM dbo.YourViewName) AS v") \
#     .option("user", "sql-admin") \
#     .option("password", "Welcome01") \
#     .load()

# df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Start

# CELL ********************

from pyspark.sql.functions import (
    col, when, substring, length, expr, to_date, 
    date_format, regexp_replace, lit
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = spark.read.table("sap.kna1").filter("MANDT = '100'")
t016t = spark.read.table("sap.t016t").filter("MANDT = '100'").filter("SPRAS = 'E'")
tvk1t = spark.read.table("sap.tvk1t").filter("MANDT = '100'").filter("SPRAS = 'E'")
tbrct = spark.read.table("sap.tbrct").filter("MANDT = '100'").filter("SPRAS = 'E'")
t077x = spark.read.table("sap.t077x").filter("MANDT = '100'").filter("SPRAS = 'E'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(kna1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# kna1 = spark.read.table("sap.kna1")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(kna1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = kna1.withColumn(
    "Customer",
    regexp_replace(col("KUNNR"), r"^0+", "")
)

kna1 = kna1.withColumn(
    "Address",
    regexp_replace(col("ADRNR"), r"^0+", "")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = kna1.withColumn(
    "Created_on",
    when(col("ERDAT") == 0, lit("1900-01-01"))
    .otherwise(to_date(col("ERDAT").cast("string"), "yyyyMMdd"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = kna1.withColumn(
    "Customer_Type",
    when(col("KTOKD") == "LEIC", lit("12"))
    .when(col("KTOKD") == "DANA", lit("13"))
    .otherwise(lit("1"))
)

kna1 = kna1.withColumn(
    "Customer_Type_Text",
    when(col("KTOKD") == "LEIC", lit("La Intercompany"))
    .when(col("KTOKD") == "DANA", lit("Dhr Intercompany"))
    .otherwise(lit("3rd Party"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = kna1.withColumn(
    "Customer_Class_Text",
    when(col("KUKLA") == "AA", "A Class, A Payer")
    .when(col("KUKLA") == "AB", "A Class, B Payer")
    .when(col("KUKLA") == "AC", "A Class, C Payer")
    .when(col("KUKLA") == "BA", "B Class, A Payer")
    .when(col("KUKLA") == "BB", "B Class, B Payer")
    .when(col("KUKLA") == "BC", "B Class, C Payer")
    .when(col("KUKLA") == "CA", "C Class, A Payer")
    .when(col("KUKLA") == "CB", "C Class, B Payer")
    .when(col("KUKLA") == "CC", "C Class, C Payer")
    .when(col("KUKLA") == "D0", "Direct Sales")
    .when(col("KUKLA") == "D1", "National Dealer")
    .when(col("KUKLA") == "D2", "Regional Dealer")
    .when(col("KUKLA") == "D3", "Local Dealer")
    .when(col("KUKLA") == "D4", "OEM")
    .when(col("KUKLA") == "D5", "SZoom Web Sales")
    .when(col("KUKLA") == "D6", "DSA")
    .when(col("KUKLA") == "L1", "Danaher/Leica ICO")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kna1 = kna1.withColumn(
    "Legal_Status_Text",
    when(col("GFORM") == "SFDC", "Inactive")
    .when(col("GFORM") == "01", "Legal Status 01")
    .when(col("GFORM") == "06", "Ltd")
    .when(col("GFORM") == "07", "Inc")
    .when(col("GFORM") == "10", "Corp")
    .when(col("GFORM") == "B", "LBS Salesforce-LBSF")
    .when(col("GFORM") == "D", "Devicor Salesforce")
    .when(col("GFORM") == "L", "LMS Salesforce-LMSF")
    .when(col("GFORM") == "X", "Don't use - iCRM")
    .when(col("GFORM") == "XB", "iCRM & LBS SFDC")
    .when(col("GFORM") == "XX", "LBSF & LMSF")
    .otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = kna1.alias("k") \
    .join(t016t.alias("t1"), col("k.BRSCH") == col("t1.BRSCH"), "left") \
    .join(tvk1t.alias("t2"), col("k.KATR1") == col("t2.KATR1"), "left") \
    .join(tbrct.alias("t3"), col("k.BRAN1") == col("t3.BRACO"), "left") \
    .join(t077x.alias("t4"), col("k.KTOKD") == col("t4.KTOKD"), "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(
    col("Created_on").alias("Created on"),
    col("k.ERNAM").alias("Created by"),
    col("Customer"),
    col("Address"),
    col("k.NAME1").alias("Name"),
    col("k.NAME2").alias("Name 2"),
    col("k.NAME3").alias("Name 3"),
    col("k.NAME4").alias("Name 4"),
    col("k.DATLT").alias("Data line"),
    col("k.STRAS").alias("Street"),
    col("k.ORT01").alias("City"),
    col("k.REGIO").alias("Region"),
    col("k.PSTLZ").alias("Postal Code"),
    col("k.LAND1").alias("Country"),
    col("k.ORT02").alias("District"),
    col("k.SORTL").alias("Search term"),
    col("k.TELF1").alias("Telephone 1"),
    col("k.TELFX").alias("Fax Number"),
    col("k.AUFSD").alias("Order block"),
    col("k.FAKSD").alias("Billing block"),
    col("k.LIFSD").alias("Delivery block"),
    col("k.CASSD").alias("Sales block"),
    col("k.LOEVM").alias("Deletion flag"),
    col("k.BEGRU").alias("Authorization"),
    col("k.BRSCH").alias("Cust segmnt 1"),
    col("t1.BRTXT").alias("Cust segmnt 1 Text"),
    col("k.KTOKD").alias("Account Group"),
    col("t4.TXT30").alias("Account Group Text"),
    col("Customer_Type"),
    col("Customer_Type_Text"),
    col("k.KUKLA").alias("Customer class."),
    col("Customer_Class_Text"),
    col("k.NIELS").alias("Cust indicator"),
    col("k.PFACH").alias("PO Box"),
    col("k.PSTL2").alias("PO Box PCode"),
    col("k.PFORT").alias("P.O.Box city"),
    col("k.SPRAS").alias("Language"),
    col("k.STCD1").alias("Tax Number 1"),
    col("k.STCD2").alias("Tax Number 2"),
    col("k.STCD3").alias("Tax Number 3"),
    col("k.LZONE").alias("Transport.zone"),
    col("k.STCEG").alias("VAT Reg. No."),
    col("k.GFORM").alias("Legal status"),
    col("Legal_Status_Text"),
    col("k.BRAN1").alias("Cust segmnt 2/3"),
    col("t3.VTEXT").alias("Cust segmnt 2/3 Text"),
    col("k.KATR1").alias("Glob.Cust.Sgmnt"),
    col("t2.VTEXT").alias("Glob.Cust.Sgmnt Text"),
    col("k.KATR2").alias("LMS OpCo"),
    col("k.KATR3").alias("LBS OpCo"),
    col("k.KATR4").alias("Acquisition Account"),
    col("k.CFOPC").alias("CFOP Category"),
    col("k.TXLW1").alias("ICMS law"),
    col("k.TXLW2").alias("IPI law")
    # col("k.ALTKN").alias("Prev.acct no."),
    # col("k.ZZSECURIMATE").alias("Securimate Prof. No.")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.customer LIMIT 1000")
display(df)

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

final_df.show(20, truncate=False)

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

final_df.columns

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

final_df.columns

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

final_df.write.mode("overwrite").saveAsTable("analytics.customer") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM analytics.customer").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("SCM_Bronze_LH.analytics.customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.customer")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.customer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC DROP TABLE IF EXISTS SCM_Bronze_LH.analytics.customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

k=spark.sql("SELECT distinct KUNNR FROM sap.kna1")
display(k)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(kna1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
ALTER TABLE SCM_Silver_LH.analytics.customer
SET TBLPROPERTIES (
 'delta.columnMapping.mode' = 'name',
 'delta.minReaderVersion' = '2',
 'delta.minWriterVersion' = '5'
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
ALTER TABLE SCM_Silver_LH.analytics.customer
DROP COLUMN LMS_OpCo, LBS_OpCo
""")

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

df = spark.table("SCM_Silver_LH.analytics.customer")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("SCM_Silver_LH.analytics.customer")

df.groupBy(df.columns).count().filter("count > 1").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
