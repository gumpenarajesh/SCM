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

from pyspark.sql import functions as F 
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_lfa1 = (
    spark.table("SCM_Bronze_LH.sap.lfa1")
    .select(
        F.col("MANDT").alias("Client"),
        F.col("LIFNR").alias("Vendor Number"),
        F.col("LAND1").alias("Country Key"),
        F.col("NAME1").alias("Name 1"),
        F.col("NAME2").alias("Name 2"),
        F.col("NAME3").alias("Name 3"),
        F.col("NAME4").alias("Name 4"),
        F.col("ORT01").alias("City"),
        F.col("ORT02").alias("District"),
        F.col("PFACH").alias("PO Box"),
        F.col("PSTL2").alias("PO Box Postal Code"),
        F.col("PSTLZ").alias("Postal Code"),
        F.col("REGIO").alias("Region"),
        F.col("SORTL").alias("Search Term"),
        F.col("STRAS").alias("Street"),
        F.col("ADRNR").alias("Address Number"),
        F.col("MCOD1").alias("Search Term 1"),
        F.col("MCOD2").alias("Search Term 2"),
        F.col("MCOD3").alias("Search Term 3"),
        F.col("ANRED").alias("Title"),
        F.col("BAHNS").alias("Train Station"),
        F.col("BBBNR").alias("International Location Number"),
        F.col("BBSNR").alias("Check Digit for ILN"),
        F.col("BEGRU").alias("Authorization Group"),
        F.col("BRSCH").alias("Industry Key"),
        F.col("BUBKZ").alias("Check Digit"),
        F.col("DATLT").alias("Data Communication Line"),
        F.col("DTAMS").alias("DME Indicator"),
        F.col("DTAWS").alias("Instruction Key"),
        F.col("ERDAT").alias("Created Date"),
        F.col("ERNAM").alias("Created By"),
        F.col("ESRNR").alias("ISR Number"),
        F.col("KONZS").alias("Group Key"),
        F.col("KTOKK").alias("Account Group"),
        F.col("KUNNR").alias("Customer Number"),
        F.col("LNRZA").alias("Alternative Payee"),
        F.col("LOEVM").alias("Deletion Flag"),
        F.col("SPERR").alias("Posting Block"),
        F.col("SPERM").alias("Purchasing Block"),
        F.col("SPRAS").alias("Language Key"),
        F.col("STCD1").alias("Tax Number 1"),
        F.col("STCD2").alias("Tax Number 2"),
        F.col("STKZA").alias("Tax Liable"),
        F.col("STKZU").alias("Tax Exempt"),
        F.col("TELBX").alias("Telebox Number"),
        F.col("TELF1").alias("Telephone 1"),
        F.col("TELF2").alias("Telephone 2"),
        F.col("TELFX").alias("Fax Number"),
        F.col("TELTX").alias("Teletex Number"),
        F.col("TELX1").alias("Telex Number"),
        F.col("XCPDK").alias("One-Time Vendor"),
        F.col("XZEMP").alias("Vendor is Employee"),
        F.col("VBUND").alias("Trading Partner"),
        F.col("FISKN").alias("Fiscal Address"),
        F.col("STCEG").alias("VAT Registration Number"),
        F.col("STKZN").alias("Natural Person"),
        F.col("SPERQ").alias("Function Block"),
        F.col("GBORT").alias("Place of Birth"),
        F.col("GBDAT").alias("Date of Birth"),
        F.col("SEXKZ").alias("Gender"),
        F.col("KRAUS").alias("Credit Information"),
        F.col("REVDB").alias("Reversal Indicator"),
        F.col("QSSYS").alias("Withholding Tax System"),
        F.col("AEDAT").alias("Changed Date"),
        F.col("USNAM").alias("Changed By"),
        F.col("VEN_CLASS").alias("Vendor Class"),
        F.col("ENTPUB").alias("Public Entity Indicator"),
        F.col("SC_CAPITAL").alias("Share Capital"),
        F.col("SC_CURRENCY").alias("Currency"),
        F.col("TRANSPORT_CHAIN").alias("Transport Chain"),
        F.col("STAGING_TIME").alias("Staging Time"),
        F.col("SCHEDULING_TYPE").alias("Scheduling Type"),
        F.col("SUBMI_RELEVANT").alias("Submission Relevant"),
        F.col("DATA_CTRLR1").alias("Data Controller 1"),
        F.col("DATA_CTRLR2").alias("Data Controller 2"),
        F.col("DATA_CTRLR3").alias("Data Controller 3"),
        F.col("DATA_CTRLR4").alias("Data Controller 4"),
        F.col("DATA_CTRLR5").alias("Data Controller 5"),
        F.col("DATA_CTRLR6").alias("Data Controller 6"),
        F.col("DATA_CTRLR7").alias("Data Controller 7"),
        F.col("DATA_CTRLR8").alias("Data Controller 8"),
        F.col("DATA_CTRLR9").alias("Data Controller 9"),
        F.col("DATA_CTRLR10").alias("Data Controller 10"),
        F.col("XDCSET").alias("Data Set Indicator"),
        F.col("ENTY_CD").alias("Entity Code"),
        F.col("RES_CNTRY").alias("Residence Country"),
        F.col("RES_REGION").alias("Residence Region"),
        F.col("CCODE").alias("Company Code")

    )
    .filter(F.col("MANDT") == "100")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in df_lfa1.columns:
    clean_name = c.strip().replace(" ", "_")
    df_lfa1 = df_lfa1.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_lfa1.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.Vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_lfa1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
