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

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Load tables
mara = spark.table("sap.MARA").filter("MANDT='100'")

makt = spark.table("sap.MAKT").filter("MANDT='100'").filter("SPRAS='E'")
# mbew = spark.table("sap.MBEW").filter("MANDT='100'")
mbew_1604 = spark.table("sap.MBEW_1604").filter("MANDT='100'")
marc = spark.table("sap.MARC").filter("MANDT='100'")
ph   = spark.table("SCM_Silver_LH.analytics.product_hierarchy")
mard = spark.table("sap.MARD").filter("MANDT='100'")
s032 = spark.table("sap.S032").filter("MANDT='100'")
# s032_1704 = spark.table("sap.S032_1704").filter("MANDT='100'")
t001w = spark.table("sap.T001W").filter("MANDT='100'")
mvke = spark.table("sap.MVKE").filter("MANDT='100'")
t134t = spark.table("sap.T134T").filter("MANDT='100'").filter("SPRAS='E'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def remove_dots(col_name):
    # Uses regex to replace all literal dots with an empty string
    return F.regexp_replace(F.col(col_name), "\\.", "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


     ph_clean = ph.withColumn(
    "Product_Hierarchy", 
    remove_dots("Product_Hierarchy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

materialdescription = (
    mara.alias("mara")
    .join(
        makt.alias("makt"),
        (col("makt.MATNR") == col("mara.MATNR")) &
        (~col("makt.MAKTX").like("%delete%")),
        "left"
    )
    .select(
        col("mara.MATNR"),
        col("makt.MAKTX").alias("Material_Text")
    )
    .distinct()
)

w_md = Window.partitionBy("MATNR").orderBy("MATNR")

materialdescription = materialdescription.withColumn(
    "rownum",
    row_number().over(w_md)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# mbew_filtered = mbew_1604.filter(col("LVORM") == "")

w_fp = Window.partitionBy(
    "MATNR", "BWKEY", "BWTAR"
).orderBy(
    concat(col("LFGJA"), col("LFMON")).desc()
)

fp = (
    mbew_1604
    .withColumn("fp_rownumber", row_number().over(w_fp))
    .filter(col("fp_rownumber") == 1)
    .select(
        "MATNR","BWKEY","LVORM","BWTAR","BKLAS","VPRSV",
        "PEINH","VERPR","STPRS","LBKUM","SALK3"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

procurement = (
    marc
    # marc.filter(col("LVORM") == "")
    .groupBy("MATNR", "WERKS")
    .agg(max("BESKZ").alias("BESKZ"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = s032.alias("s032") \
    .join(mara.alias("mara"),
          col("mara.MATNR") == col("s032.MATNR"),
          "left") \
    .join(materialdescription.alias("makt"),
          (col("makt.MATNR") == col("mara.MATNR")) &
          (col("makt.rownum") == 1),
          "left") \
    .join(mard.alias("mard"),
          (col("mard.MATNR") == col("s032.MATNR")) &
          (col("mard.WERKS") == col("s032.WERKS")) &
          (col("mard.LGORT") == col("s032.LGORT")) ,
      #     (col("mard.LVORM") == ""),
          "inner") \
    .join(fp.alias("mbew"),
          (col("mbew.MATNR") == col("s032.MATNR")) &
          (col("mbew.BWKEY") == col("s032.WERKS")),
      #     (col("mbew.LVORM") == ""),
          "left") \
    .join(procurement.alias("marc"),
          (col("marc.MATNR") == col("mbew.MATNR")) &
          (col("marc.WERKS") == col("mbew.BWKEY")),
          "left") \
    .join(t001w.alias("t001w"),
          col("t001w.BWKEY") == col("mbew.BWKEY"),
          "left") \
    .join(mvke.alias("mvke"),
        (col("mvke.VKORG") == col("t001w.VKORG")) &
          (col("mvke.MATNR") == col("mara.MATNR")) &
          (col("mvke.LVORM") == "")& 
          (col("mvke.VMSTA") != ""),
          "left") \
.join(t134t.alias("t134t"),
            col("t134t.MTART") == col("mara.MTART"),
            "left")\
 .join(ph_clean.alias("ph1"),F.col("mara.PRDHA") == F.col("ph1.Product_Hierarchy"),"left")
       
         

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=df.select(

    col("t001w.VKORG").alias("Sales org"),
    col("s032.WERKS").alias("Plant"),
    col("mard.LGORT").alias("Stor Location"),
    col("mard.LGPBE").alias("Storage Bin"),
# Remove leading zeros
    regexp_replace(col("mara.MATNR"), "^0+", "").alias("Material"),

    col("makt.Material_Text").alias("Material Text"),
    col("marc.BESKZ").alias("Procurement"),

    col("mara.MTART").alias("Material Type"),
    col("t134t.MTBEZ").alias("Material Type Text"),
    col("mara.MATKL").alias("Material group"),
    col("mara.SPART").alias("Product group"),
    #col("product_hierarchy.PH_Product_Line_Text").alias("Product_Line_Text"),
    F.col("ph1.PH_Product_Line_Text").alias("PH_Product_Line_Text"),

    col("mvke.MTPOS").alias("Item cat group"),
    col("mvke.VMSTA").alias("Prod Life Cycle"),

    col("mbew.BKLAS").alias("Valuation Class"),
    col("mbew.VPRSV").alias("Price Control"),
     col("mbew.PEINH").cast("decimal(15,2)").alias("Price Unit"),
    col("mbew.VERPR").cast("decimal(15,2)").alias("Moving price"),
    col("mbew.STPRS").cast("decimal(15,2)").alias("Standard price"),

    
    col("mbew.LBKUM").cast("decimal(15,2)").alias("Total Stock"),
    col("mbew.SALK3").cast("decimal(15,2)").alias("Total Value"),
    col("mard.LABST").cast("decimal(15,2)").alias("Unrestricted"),

    when(col("s032.LETZTZUG") == 0, None)
    .otherwise(to_date(col("s032.LETZTZUG").cast("string"), "yyyyMMdd"))
    .alias("Last Receipt"),

    when(col("s032.LETZTABG") == 0, None)
    .otherwise(to_date(col("s032.LETZTABG").cast("string"), "yyyyMMdd"))
    .alias("Last gds issue"),

    when(col("s032.LETZTVER") == 0, None)
    .otherwise(to_date(col("s032.LETZTVER").cast("string"), "yyyyMMdd"))
    .alias("Last consumption"),

    when(col("s032.LETZTBEW") == 0, None)
    .otherwise(to_date(col("s032.LETZTBEW").cast("string"), "yyyyMMdd"))
    .alias("Last gds mvmt"),

    col("s032.HWAER").alias("Currency"),
    col("mard.LFGJA").alias("Year cur period"),
    col("mard.LFMON").alias("Current period"),

    concat(col("s032.MATNR"), col("s032.WERKS"), col("s032.LGORT"))
    .alias("INTEGRATION ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

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

for c in df.columns:
    clean_name = c.strip().replace(" ", "_")
    df = df.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

storage_locations = [
    '7630','7640','7650','7660','7670','7680','7690',
    '5550','5551','5552','5553','5554','5556','5557','5558',
    '3120','3121','3122','3123','3124','3125'
]

filtered_df1 = df.filter(df.Stor_Location.isin(storage_locations))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(filtered_df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_clean = df.na.drop()
# display(df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# cols = [
#  'Sales_org', 'Plant', 'Stor_Location', 'Storage_Bin', 'Material', 'Material_Text',
#  'Procurement', 'Material_Type', 'Material_group', 'Product_group', 'Item_cat_group',
#   'Valuation_Class', 'Price_Control', 'Price_Unit', 'Moving_price',
#  'Standard_price', 'Total_Stock', 'Total_Value', 'Unrestricted', 'Last_Receipt',
#  'Last_gds_issue', 'Last_consumption', 'Last_gds_mvmt', 'Currency', 'Year_cur_period',
#  'Current_period', 'INTEGRATION_ID'
# ]

# df_clean = df
# for c in cols:
#     df_clean = df_clean.filter(
#         F.col(c).isNotNull() & (F.trim(F.col(c)) != "")
#     )
# display(df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df1.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.current_stock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
