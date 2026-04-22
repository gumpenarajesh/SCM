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

lfa1_df = spark.read.table("SCM_Bronze_LH.sap.LFA1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor_df = lfa1_df.select(

    col("LIFNR").alias("Key"),
    col("MANDT").alias("Client"),

    regexp_replace(col("LIFNR"), "^0+", "").alias("Vendor"),

    col("LAND1").alias("Country"),
    col("NAME1").alias("Name"),
    col("NAME2").alias("Name 2"),
    col("NAME3").alias("Name 3"),
    col("NAME4").alias("Name 4"),

    col("ORT01").alias("City"),
    col("ORT02").alias("District"),

    col("PFACH").alias("PO Box"),
    col("PSTL2").alias("PO Box PCode"),
    col("PSTLZ").alias("Postal Code"),

    col("REGIO").alias("Region"),
    col("SORTL").alias("Search term"),
    col("STRAS").alias("Street"),

    regexp_replace(col("ADRNR"), "^0+", "").alias("Address"),

    col("ANRED").alias("Title"),
    col("BAHNS").alias("Train station"),
    col("BEGRU").alias("Authorization"),
    col("BRSCH").alias("Cust segmnt 1"),
    col("DATLT").alias("Data line"),
    col("DTAWS").alias("Instruction key"),

    when(col("ERDAT")==0,"1900-01-01")
    .otherwise(to_date(col("ERDAT").cast("string"),"yyyyMMdd"))
    .alias("Created on"),

    col("ERNAM").alias("Created by"),
    col("ESRNR").alias("ISR Number"),
    col("KONZS").alias("Corporate Group"),
    col("KTOKK").alias("Vendor type"),
    col("KUNNR").alias("Customer"),

    col("LNRZA").alias("Alternat. payee"),
    col("LOEVM").alias("Deletion flag"),
    col("SPERR").alias("Posting Block"),
    col("SPERM").alias("Purch. block"),
    col("SPRAS").alias("Language"),

    col("STCD1").alias("Tax Number 1"),
    col("STCD2").alias("Tax Number 2"),

    col("TELBX").alias("Telebox"),
    col("TELF1").alias("Telephone 1"),
    col("TELF2").alias("Telephone 2"),
    col("TELFX").alias("Fax Number"),
    col("TELTX").alias("Teletext"),
    col("TELX1").alias("Telex"),

    col("XZEMP").alias("Payee in doc."),
    col("VBUND").alias("Trading Partner"),
    col("FISKN").alias("Fiscal address"),
    col("STCEG").alias("VAT Reg. No."),
    col("STKZN").alias("Natural person"),

    col("SPERQ").alias("Block function"),
    col("KRAUS").alias("Cred.info no."),

    when(col("REVDB")==0,"1900-01-01")
    .otherwise(to_date(col("REVDB").cast("string"),"yyyyMMdd"))
    .alias("Last ext.review"),

    col("QSSYS").alias("Actual QM sys."),
    col("PFORT").alias("P.O.Box city"),
    col("LTSNA").alias("VSR relevant"),
    col("WERKR").alias("Plant relevant"),
    col("DUEFL").alias("Data Transfer Status"),
    col("TXJCD").alias("Tax Jur."),

    col("LFURL").alias("URL"),

    col("J_1KFREPRE").alias("Rep's Name"),
    col("J_1KFTBUS").alias("Type of Business"),
    col("J_1KFTIND").alias("Type of Industry"),

    col("CONFS").alias("Confirm.status"),

    when(col("UPDAT")==0,"1900-01-01")
    .otherwise(to_date(col("UPDAT").cast("string"),"yyyyMMdd"))
    .alias("Confirm.date"),

    col("UPTIM").alias("confirm.time"),
    col("NODEL").alias("Deletion block"),

    when(col("QSSYSDAT")==0,"1900-01-01")
    .otherwise(to_date(col("QSSYSDAT").cast("string"),"yyyyMMdd"))
    .alias("QM system to"),

    col("STENR").alias("Tax Number"),
    # col("ZZQARACLASSF").alias("QA/RA Classif.")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in vendor_df.columns:
    clean_name = c.strip().replace(" ", "_")
    vendor_df = vendor_df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor_df.write.mode("overwrite").saveAsTable("SCM_Silver_LH.ops.Vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(vendor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("SCM_Silver_LH.ops.vendor")

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
