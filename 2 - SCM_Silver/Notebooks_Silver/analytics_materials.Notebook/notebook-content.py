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
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara = spark.table("sap.mara").filter("MANDT = '100'")
makt = spark.table("sap.makt").filter("MANDT = '100'").filter("SPRAS ='E'")
mgef = spark.table("sap.mgef").filter("MANDT = '100'")
ent1079 = spark.table("sap.ent1079").filter("MANDT = '100'")
t134t = spark.table("sap.t134t").filter("MANDT = '100'").filter("SPRAS = 'E'")
t137t = spark.table("sap.t137t").filter("MANDT = '100'").filter("SPRAS = 'E'")
t023t = spark.table("sap.t023t").filter("MANDT = '100'").filter("SPRAS = 'E'")
twewt = spark.table("sap.twewt").filter("MANDT = '100'").filter("SPRAS = 'E'")
# znemonpol = spark.table("sap.znemonpol")
product_hierarchy = spark.table("SCM_Silver_LH.analytics.product_hierarchy")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_makt = Window.partitionBy("MATNR").orderBy("MATNR")

materialdescription = (
    makt
    .filter(~F.lower(F.col("MAKTX")).contains("delete"))
    .select("MATNR","MAKTX")
    .dropDuplicates()
    .withColumn("rownum",F.row_number().over(window_makt))
    .filter(F.col("rownum")==1)
    .drop("rownum")
    .withColumnRenamed("MAKTX","Material_Text")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_mgef = Window.partitionBy("STOFF").orderBy("STOFF")

hazardousmaterialdescription = (
    mgef
    .select("STOFF","REGKZ","STOFT","LAGKL","LGFVM")
    .withColumn("ROWNUM",F.row_number().over(window_mgef))
    .filter(F.col("ROWNUM")==1)
    .drop("ROWNUM")
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

mara = (
    mara
    .withColumn(
        "Created_On",
        F.when(F.col("ERSDA")==0, F.lit("1900-01-01"))
        .otherwise(F.to_date(F.col("ERSDA").cast("string"),"yyyyMMdd"))
    )
    .withColumn(
        "Last_Change",
        F.when(F.col("LAEDA")==0, F.lit("1900-01-01"))
        .otherwise(F.to_date(F.col("LAEDA").cast("string"),"yyyyMMdd"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# mara = mara.filter(F.col("INTEGRATION_INCLUDE")==1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

t134t_filtered = t134t.filter(
    (t134t.SPRAS == "E") &
    (t134t.MANDT == "100")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    mara
    .join(materialdescription,"MATNR","left")
    .join(broadcast(product_hierarchy),
          mara.PRDHA == product_hierarchy["Product_Hierarchy"],
          "left")

    .join(broadcast(ent1079),
          mara.LABOR == ent1079.LABOR,
          "left")

    # .join(
    # broadcast(t134t),
    
    # mara.MTART == t134t.MTART,
    # "left")
    .join(
    broadcast(t134t_filtered),
    mara.MTART == t134t_filtered.MTART,
    
    "left"
)


    .join(broadcast(t137t),
          mara.MBRSH == t137t.MBRSH,
          "left")

    # .join(broadcast(t023t),
    #       mara.MATKL == t023t.MATKL,
    #       "left")
    .join(
    broadcast(t023t),
    (mara.MATKL == t023t.MATKL) &
    (t023t.SPRAS == "E") &
    (t023t.MANDT == "100"),
    "left"
)      

    .join(broadcast(twewt),
          mara.EXTWG == twewt.EXTWG,
          "left")

    .join(hazardousmaterialdescription,
          mara.STOFF == hazardousmaterialdescription.STOFF,
          "left")

    # .join(znemonpol,"MATNR","left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("MATNR", F.regexp_replace("MATNR", "^0+", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(
    F.col("MATNR").alias("Material"),
    
    "Material_Text",
    "Created_On",
    mara.ERNAM.alias("Created_by"),
    "Last_Change",
    mara.AENAM.alias("Changed_by"),
    mara.VPSTA.alias("Complete_status"),
    mara.PSTAT.alias("Maint_status"),
    mara.LVORM.alias("DF_client_level"),
    mara.MTART.alias("Material_Type"),
    t134t.MTBEZ.alias("Material_Type_Text"),
    mara.MATKL.alias("Material_Group"),
    t023t.WGBEZ.alias("Material_Group_Text"),
    mara.MEINS.alias("Base_Unit"),
    mara.BRGEW.alias("Gross_Weight"),
    mara.NTGEW.alias("Net_Weight"),
    mara.GEWEI.alias("Weight_unit"),
    mara.VOLUM.alias("Volume"),
    mara.VOLEH.alias("Volume_unit"),
    mara.PRDHA.alias("Product_Hierarchy"),
    hazardousmaterialdescription.STOFT.alias("Haz_Material_Text"),
    hazardousmaterialdescription.LAGKL.alias("Storage_Class"),
    hazardousmaterialdescription.LGFVM.alias("Hazard_Warning"),



    # znemonpol.ZANIMAL.alias("Animal")

    mara.BISMT.alias("Old_matl_number"),
mara.NORMT.alias("Tradename"),
mara.LABOR.alias("Article_Type"),
ent1079.LBTXT.alias("Article_Type_Text"),
mara.BEHVO.alias("Container"),
mara.TEMPB.alias("Temperature"),
mara.SPART.alias("Product_group"),
mara.EAN11.alias("EAN_UPC"),
mara.NUMTP.alias("EAN_Category"),
mara.LAENG.alias("Length"),
mara.BREIT.alias("Width"),
mara.HOEHE.alias("Height"),
mara.MEABM.alias("Unit"),
mara.XCHPF.alias("BatchManagement"),
mara.BEGRU.alias("AuthorizGroup"),
mara.EXTWG.alias("Ext_Matl_Group"),
twewt.EWBEZ.alias("Ext_Matl_Group_Text"),
mara.MHDRZ.alias("Rem_Shelf_life"),
mara.IPRKZ.alias("Period_Ind"),
mara.MHDHB.alias("Tot_shelf_life")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable("SCM_Silver_LH.analytics.material")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SCM_Silver_LH.analytics.material LIMIT 1000")
display(df)

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

print("MARA", mara.count())

df1 = mara.join(materialdescription,"MATNR","left")
print("After MAKT", df1.count())

df2 = df1.join(t134t, mara.MTART == t134t.MTART,"left")
print("After T134T", df2.count())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara.join(materialdescription,"MATNR","left").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara.join(materialdescription,"MATNR","left") \
    .join(broadcast(t134t_filtered), mara.MTART == t134t_filtered.MTART,"left").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara.join(materialdescription,"MATNR","left") \
    .join(broadcast(t134t_filtered), mara.MTART == t134t_filtered.MTART,"left") \
 .count()

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
