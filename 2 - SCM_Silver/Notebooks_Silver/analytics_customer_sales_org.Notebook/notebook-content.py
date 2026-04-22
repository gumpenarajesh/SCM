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

from pyspark.sql.functions import col, expr, when, to_date, lpad
from pyspark.sql.types import DecimalType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def remove_leading_zeros(column_name):
    return expr(f"substring({column_name}, regexp_instr({column_name} || '.', '[^0]'), length({column_name}))")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = spark.table("SCM_Silver_LH.analytics.Customer")
knvh_df = spark.table("SCM_Bronze_LH.sap.KNVH").filter("MANDT='100'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cust_knvh_df = customer_df.alias("c").join(
    knvh_df.alias("k"),
    col("c.Customer") == remove_leading_zeros("k.KUNNR"),
    "left"
)
cust_knvh_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_h_df = customer_df.alias("ch")

cust_hierarchy_df = cust_knvh_df.join(
    customer_h_df,
    col("ch.Customer") == remove_leading_zeros("k.HKUNNR"),
    "left"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# cust_hierarchy_df = cust_hierarchy_df.filter(
#     (col("k.DATBI") == "99991231") &
#     (col("k.HITYP") == "Z") &
#     (col("k.HKUNNR") != "")
# )
# cust_hierarchy_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_hierarchies = cust_hierarchy_df.select(
    col("k.HKUNNR").alias("HglvCust."),
    col("ch.Name").alias("HglvCust. Text"),
    col("c.Customer").alias("Customer"),
    col("c.Name"),
    col("k.VKORG").alias("Sales Org."),
    
    col("c.Cust_segmnt_1").alias("Cust segmnt 1"),
    col("c.Cust_segmnt_1_Text").alias("Cust segmnt 1 Text"),
    col("c.Account_Group").alias("Account Group"),
    col("c.Account_Group_Text").alias("Account Group Text"),
    
    col("ch.Cust_segmnt_1").alias("Cust segmnt 1 HglvCust."),
    col("ch.Cust_segmnt_1_Text").alias("Cust segmnt 1 Text HglvCust."),
    col("ch.Account_Group").alias("Account Group HglvCust."),
    col("ch.Account_Group_Text").alias("Account Group Text HglvCust."),
    
    col("k.VTWEG").alias("Distr. Channel"),
    col("k.SPART").alias("Product group")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cust_hierarchy_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_hierarchies.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

knvv_df = spark.table("SCM_Bronze_LH.sap.KNVV").alias("knvv").filter("MANDT='100'")
tvkdt_df = spark.table("SCM_Bronze_LH.sap.TVKDT").alias("tvkdt").filter("MANDT='100'").filter("SPRAS='E'")
sales_org_df = spark.table("SCM_Silver_LH.analytics.Sales_Organization").alias("so")
knkk_df = spark.table("SCM_Bronze_LH.sap.KNKK").alias("knkk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(knvv_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES IN analytics").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tvkdt_df = tvkdt_df.filter(~col("KALKS").isin("01", "02"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = knvv_df.join(
    tvkdt_df,
    col("tvkdt.KALKS") == col("knvv.KALKS"),
    "left"
)


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

display(tvkdt_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = main_df.join(
    sales_org_df.alias("so"),
    col("so.Sales_Org") == col("knvv.VKORG"),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = main_df.join(
    knkk_df.alias("knkk"),
    (col("knkk.KKBER") == col("so.Chart_Of_Accts")) &
    (col("knkk.KUNNR") == col("knvv.KUNNR")),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = main_df.join(
    customer_hierarchies.alias("ch"),
    (remove_leading_zeros("knvv.KUNNR") == col("ch.Customer")) &
    (col("knvv.VKORG") == col("ch.`Sales Org.`")) &
    (col("knvv.VTWEG") == col("ch.`Distr. Channel`")) &
    (col("knvv.SPART") == col("ch.`Product group`")),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import DecimalType

final_df = main_df.select(

    col("knvv.MANDT").alias("Client"),
    col("knvv.VKORG").alias("Sales Org."),
    remove_leading_zeros("knvv.KUNNR").alias("Customer"),

    col("ch.`Cust segmnt 1`"),
    col("ch.`Cust segmnt 1 Text`"),
    col("ch.`Account Group`"),
    col("ch.`Account Group Text`"),

    remove_leading_zeros("ch.`HglvCust.`").alias("HglvCust."),
    col("ch.`HglvCust. Text`"),

    col("ch.`Cust segmnt 1 HglvCust.`"),
    col("ch.`Cust segmnt 1 Text HglvCust.`"),
    col("ch.`Account Group HglvCust.`"),
    col("ch.`Account Group Text HglvCust.`"),

    col("knvv.VTWEG").alias("Distr. Channel"),
    col("knvv.SPART").alias("Product group"),
    col("knvv.ERNAM").alias("Created by"),

    when(col("knvv.ERDAT") == 0, "1900-01-01")
    .otherwise(to_date(col("knvv.ERDAT").cast("string"), "yyyyMMdd"))
    .alias("Created on"),

    col("knvv.KALKS").alias("Cust.pric.proc."),
    col("tvkdt.VTEXT").alias("Cust.pric.proc. Text"),
    col("knvv.KONDA").alias("Price group"),
    col("knvv.PLTYP").alias("Price List"),

    col("knvv.INCO1").alias("Incoterms"),
    col("knvv.INCO2").alias("Incoterms 2"),

    col("knvv.LIFSD").alias("DelBlckSalesAr."),
    col("knvv.VSBED").alias("Shipping Cond."),

    col("knkk.KLIMK").cast(DecimalType(15,2)).alias("Credit limit"),

    col("knvv.WAERS").alias("Currency"),
    col("knvv.KTGRD").alias("CustAcctAssgGrp"),
    # col("knvv.ZKOND").alias("US Price Group"),
    # col("knvv.ZHNDCHRG").alias("HndlChrg Exempt"),
    # col("knvv.ZZTRATY").alias("Means of transport"),
    # col("knvv.ZZGPOID").alias("GPO ID"),
    # col("knvv.SDABW").alias("Spec.processing"),
    col("knvv.ZTERM").alias("Payt Terms")
)

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

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.filter(
    col("`Cust segmnt 1`").isNotNull() &
    col("`Cust segmnt 1 Text`").isNotNull() &
    col("`Account Group`").isNotNull() &
    col("`Account Group Text`").isNotNull() &
    col("`HglvCust.`").isNotNull() &
    col("`HglvCust. Text`").isNotNull() &
    col("`Cust segmnt 1 HglvCust.`").isNotNull() &
    col("`Cust segmnt 1 Text HglvCust.`").isNotNull() &
    col("`Account Group HglvCust.`").isNotNull() &
    col("`Account Group Text HglvCust.`").isNotNull()
)

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

final_df.columns

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

final_df.write.mode("overwrite").saveAsTable("SCM_Silver_LH.analytics.Customer_Sales_org") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
