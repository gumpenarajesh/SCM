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

# MARKDOWN ********************

# # Start

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    substring,
    concat,
    row_number,
    desc,
    sum as _sum,
    max as _max,
    min as _min,
    to_date,
    date_add,
    current_date,
    format_string,
    regexp_replace,
    expr,
    lpad
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType,
    DateType,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekko  = spark.table("SCM_Bronze_LH.sap.EKKO").filter("MANDT='100'")
ekpo  = spark.table("SCM_Bronze_LH.sap.EKPO").filter("MANDT='100'")
ekes  = spark.table("SCM_Bronze_LH.sap.EKES").filter("MANDT='100'")
marc  = spark.table("SCM_Bronze_LH.sap.MARC").filter("MANDT='100'")
ekbe  = spark.table("SCM_Bronze_LH.sap.EKBE").filter("MANDT='100'")
eket  = spark.table("SCM_Bronze_LH.sap.EKET").filter("MANDT='100'")
lfa1  = spark.table("SCM_Bronze_LH.sap.LFA1").filter("MANDT='100'")
t001l = spark.table("SCM_Bronze_LH.sap.T001L").filter("MANDT='100'")
mara  = spark.table("SCM_Bronze_LH.sap.MARA").filter("MANDT='100'")
likp  = spark.table("SCM_Bronze_LH.sap.LIKP").filter("MANDT='100'")
eikp  = spark.table("SCM_Bronze_LH.sap.EIKP").filter("MANDT='100'")
lips  = spark.table("SCM_Bronze_LH.sap.LIPS").filter("MANDT='100'")
eipo  = spark.table("SCM_Bronze_LH.sap.EIPO").filter("MANDT='100'")
rbkp  = spark.table("SCM_Bronze_LH.sap.RBKP").filter("MANDT='100'")
ekkn  = spark.table("SCM_Bronze_LH.sap.EKKN").filter("MANDT='100'")
mbew  = spark.table("SCM_Bronze_LH.sap.MBEW").filter("MANDT='100'")
makt  = spark.table("SCM_Bronze_LH.sap.MAKT").filter("MANDT='100'").filter("SPRAS='E'")
t001w = spark.table("SCM_Bronze_LH.sap.T001W").filter("MANDT='100'")
konp  = spark.table("SCM_Bronze_LH.sap.KONP").filter("MANDT='100'")
a018  = spark.table("SCM_Bronze_LH.sap.A018").filter("MANDT='100'")
# a988  excluded
# a984  excluded
# a998  excluded
# a985  excluded
# t024x excluded
# t027b excluded

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekes_w = Window.partitionBy("EBELN", "EBELP").orderBy(col("ETENS").desc())

ekes_f = (
    ekes
    .filter(col("XBLNR") != "")
    .withColumn("RN", row_number().over(ekes_w))
    .filter(col("RN") == 1)
    .select("EBELN", "EBELP", "ETENS", "XBLNR", "ERDAT", "VBELN")
    .alias("ekes")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

marc_w = Window.partitionBy("MATNR", "WERKS").orderBy(col("MATNR").desc(), col("WERKS").desc())

marc_f = (
    marc
    .withColumn("RN", row_number().over(marc_w))
    .filter(col("RN") == 1)
    .select("MATNR", "WERKS", "DISPO", "STAWN", "MAABC", "BESKZ", "FEVOR", "WEBAZ")
    .alias("marc")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekbe_gr = (
    ekbe
    .filter(col("VGABE") == "1") #Goods Receipt
    .groupBy("EBELN", "EBELP")
    .agg(
        _max("BUDAT").alias("GR_BUDAT"),
        _sum(when(col("SHKZG") == "H", -col("MENGE")).otherwise(col("MENGE"))).alias("GR_MENGE"),
        _sum("WESBS").alias("GR_WESBS")
    )
    .alias("ekbe_gr")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekbe_ir = (
    ekbe
    .filter(col("VGABE") == "2") #Invoice Receipt
    .groupBy("EBELN", "EBELP")
    .agg(
        _max("BUDAT").alias("IR_BUDAT"),
        _sum(when(col("SHKZG") == "H", -col("MENGE")).otherwise(col("MENGE"))).alias("IR_MENGE"),
        _sum("WESBS").alias("IR_WESBS"),
        _max("BELNR").alias("BELNR"),
        _max("GJAHR").alias("GJAHR")
    )
    .alias("ekbe_ir")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekbe6_w = Window.partitionBy("EBELN", "EBELP").orderBy(col("BUZEI").desc())

ekbe_gi = (
    ekbe
    .filter(col("VGABE") == "6") 
    .withColumn("MENGE", when(col("SHKZG") == "H", -col("MENGE")).otherwise(col("MENGE")))
    .withColumn("RN", row_number().over(ekbe6_w))
    .filter(col("RN") == 1)
    .select("EBELN", "EBELP", "BUDAT", "MENGE", "WESBS")
    .alias("ekbe_gi")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lips_w = Window.partitionBy("VBELN", "MATNR").orderBy(col("POSNR").desc())

lips_f = (
    lips
    .withColumn("RN", row_number().over(lips_w))
    .filter(col("RN") == 1)
    .select("VBELN", "MATNR", "POSNR")
    .alias("lips")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eipo_w = Window.partitionBy("EXNUM", "EXPOS").orderBy(col("EXPOS").desc())

eipo_f = (
    eipo
    .withColumn("RN", row_number().over(eipo_w))
    .filter(col("RN") == 1)
    .select("EXNUM", "EXPOS", "STAWN")
    .alias("eipo")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mbew_w = Window.partitionBy("MATNR", "BWKEY", "BWTAR") \
    .orderBy(F.concat(col("LFGJA"), col("LFMON")).desc())

mbew_f = (
    mbew
    .filter(col("LVORM") == "")
    .withColumn("RN", row_number().over(mbew_w))
    .filter(col("RN") == 1)
    .select("MATNR", "BWKEY", "BWTAR", "PEINH", "VERPR", "STPRS", "BKLAS")
    .alias("mbew")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

makt_w = Window.partitionBy("MATNR").orderBy("MATNR")

makt_f = (
    makt
    .filter(~col("MAKTX").like("%delete%"))
    .withColumn("RN", row_number().over(makt_w))
    .filter(col("RN") == 1)
    .select("MATNR", "MAKTX")
    .alias("makt")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mat_overhead_material = (
    a018
    .join(konp, a018["KNUMH"] == konp["KNUMH"])
    .filter(
        (a018["KSCHL"] == "ZOSO") &
        (F.to_date(a018["DATBI"].cast("string"), "yyyyMMdd") >= F.current_date()) &
        (F.to_date(a018["DATAB"].cast("string"), "yyyyMMdd") <= F.current_date())
    )
    .select(
        a018["LIFNR"].alias("MOM_LIFNR"),
        a018["MATNR"].alias("MOM_MATNR"),
        a018["EKORG"].alias("MOM_EKORG"),
        a018["DATBI"].alias("MOM_DATBI"),
        a018["DATAB"].alias("MOM_DATAB"),
        konp["KBETR"].alias("MOM_KBETR")
    )
    .select(
        col("MOM_LIFNR").alias("LIFNR"),
        col("MOM_MATNR").alias("MATNR"),
        col("MOM_EKORG").alias("EKORG"),
        col("MOM_DATBI").alias("DATBI"),
        col("MOM_DATAB").alias("DATAB"),
        col("MOM_KBETR").alias("KBETR")
    )
    .alias("mat_overhead_material")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

freight_material = (
    a018
    .join(konp, a018["KNUMH"] == konp["KNUMH"])
    .filter(
        (a018["KSCHL"] == "ZOSF") &
        (F.to_date(a018["DATBI"].cast("string"), "yyyyMMdd") >= F.current_date()) &
        (F.to_date(a018["DATAB"].cast("string"), "yyyyMMdd") <= F.current_date())
    )
    .select(
        a018["LIFNR"].alias("FM_LIFNR"),
        a018["MATNR"].alias("FM_MATNR"),
        a018["EKORG"].alias("FM_EKORG"),
        a018["DATBI"].alias("FM_DATBI"),
        a018["DATAB"].alias("FM_DATAB"),
        a018["ESOKZ"].alias("FM_ESOKZ"),
        konp["KBETR"].alias("FM_KBETR")
    )
)

freight_mat_w = Window.partitionBy("FM_LIFNR", "FM_MATNR", "FM_EKORG").orderBy(col("FM_ESOKZ").desc())

freight_material = (
    freight_material
    .withColumn("RN", row_number().over(freight_mat_w))
    .filter(col("RN") == 1)
    .select(
        col("FM_LIFNR").alias("LIFNR"),
        col("FM_MATNR").alias("MATNR"),
        col("FM_EKORG").alias("EKORG"),
        col("FM_DATBI").alias("DATBI"),
        col("FM_DATAB").alias("DATAB"),
        col("FM_KBETR").alias("KBETR")
    )
    .alias("freight_material")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ekko_a  = ekko.alias("ekko")
ekpo_a  = ekpo.alias("ekpo")
lfa1_a  = lfa1.alias("lfa1")
t001l_a = t001l.alias("t001l")
mara_a  = mara.alias("mara")
eket_a  = eket.alias("eket")
likp_a  = likp.alias("likp")
rbkp_a  = rbkp.alias("rbkp")
ekkn_a  = ekkn.alias("ekkn")
t001w_a = t001w.alias("t001w")
eikp_a  = eikp.alias("eikp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    ekko_a

    .join(ekpo_a, col("ekko.EBELN") == col("ekpo.EBELN"), "left")

    .join(lfa1_a, col("ekko.LIFNR") == col("lfa1.LIFNR"), "left")

    .join(t001l_a,
          (col("ekpo.WERKS") == col("t001l.WERKS")) &
          (col("ekpo.LGORT") == col("t001l.LGORT")),
          "left")

    .join(mara_a, col("ekpo.MATNR") == col("mara.MATNR"), "left")

    .join(marc_f,
          (col("ekpo.MATNR") == col("marc.MATNR")) &
          (col("ekpo.WERKS") == col("marc.WERKS")),
          "left")

    .join(ekes_f,
          (col("ekpo.EBELN") == col("ekes.EBELN")) &
          (col("ekpo.EBELP") == col("ekes.EBELP")),
          "left")

    .join(eket_a,
          (col("ekpo.EBELN") == col("eket.EBELN")) &
          (col("ekpo.EBELP") == col("eket.EBELP")),
          "left")

    .join(likp_a, col("ekes.VBELN") == col("likp.VBELN"), "left")

    .join(ekbe_gr,
          (col("ekpo.EBELN") == col("ekbe_gr.EBELN")) &
          (col("ekpo.EBELP") == col("ekbe_gr.EBELP")),
          "left")

    .join(ekbe_ir,
          (col("ekpo.EBELN") == col("ekbe_ir.EBELN")) &
          (col("ekpo.EBELP") == col("ekbe_ir.EBELP")),
          "left")

 
    .join(ekbe_gi,
          (col("ekpo.EBELN") == col("ekbe_gi.EBELN")) &
          (col("ekpo.EBELP") == col("ekbe_gi.EBELP")),
          "left")

   
    .join(eikp_a, col("ekes.XBLNR") == col("eikp.REFNR"), "left")

 
    .join(lips_f,
          (col("ekes.VBELN") == col("lips.VBELN")) &
          (col("ekpo.MATNR") == col("lips.MATNR")),
          "left")

    .join(eipo_f,
          (col("eikp.EXNUM") == col("eipo.EXNUM")) &
          (col("lips.POSNR") == col("eipo.EXPOS")),
          "left")

    .join(rbkp_a,
          (col("ekbe_ir.BELNR") == col("rbkp.BELNR")) &
          (col("ekbe_ir.GJAHR") == col("rbkp.GJAHR")),
          "left")

    .join(ekkn_a,
          (col("ekpo.EBELN") == col("ekkn.EBELN")) &
          (col("ekpo.EBELP") == col("ekkn.EBELP")) &
          (col("ekkn.VBELP") != "000000"),
          "left")

    .join(makt_f, col("ekpo.MATNR") == col("makt.MATNR"), "left")

    .join(mbew_f,
          (col("ekpo.MATNR") == col("mbew.MATNR")) &
          (col("ekpo.WERKS") == col("mbew.BWKEY")) &
          (col("mbew.BWTAR") == ""),
          "left")

    .join(t001w_a,
          col("t001w.LIFNR") == when(col("ekko.LIFNR") == "", lit("0")).otherwise(col("ekko.LIFNR")),
          "left")

    .join(mat_overhead_material,
          (col("mat_overhead_material.LIFNR") == col("ekko.LIFNR")) &
          (col("mat_overhead_material.EKORG") == col("ekko.EKORG")) &
          (col("mat_overhead_material.MATNR") == col("ekpo.MATNR")),
          "left")

    .join(freight_material,
          (col("freight_material.LIFNR") == col("ekko.LIFNR")) &
          (col("freight_material.EKORG") == col("ekko.EKORG")) &
          (col("freight_material.MATNR") == col("ekpo.MATNR")),
          "left")

    .filter(col("ekpo.LOEKZ") != "L")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sap_date(c):
    return (
        when(c.cast("long") == 0, F.to_date(lit("1900-01-01")))
        .otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_div(numerator, denominator):
    """Returns None when denominator is 0, avoiding division by zero."""
    return numerator / when(denominator == 0, None).otherwise(denominator)

final_df = df.select(
    col("ekko.EKORG").alias("Purch. Organization"),
    col("ekko.ERNAM").alias("Created by"),
    sap_date(col("ekko.AEDAT")).alias("Created on"),
    col("ekko.EBELN").alias("Purchasing Document"),
    expr("""
        substring(ekpo.EBELP,
            length(ekpo.EBELP) - length(ltrim('0', ekpo.EBELP)) + 1,
            length(ekpo.EBELP))
    """).alias("Item"),

    col("ekko.BSART").alias("Purchasing Doc. Type"),
    col("marc.WERKS").alias("Plant"),
    col("ekpo.LGORT").alias("Storage Location"),
    col("t001l.LGOBE").alias("Storage Location Text"),
    col("marc.DISPO").alias("MRP Controller"),
    col("ekko.LIFNR").alias("Vendor"),
    col("lfa1.NAME1").alias("Vendor Name"),
    col("lfa1.LAND1").alias("Vendor Country"),
    # when(col("ekko.ZZSERVICE") == "X", lit("SERVICE")).otherwise(lit("")).alias("Service"),

    col("mara.LABOR").alias("Article type"),
    col("mara.SPART").alias("Product group"),
    substring(col("mara.PRDHA"), 1, 4).alias("Product line"),

    expr("""
        substring(ekpo.MATNR,
            length(ekpo.MATNR) - length(ltrim('0', ekpo.MATNR)) + 1,
            length(ekpo.MATNR))
    """).alias("Material"),

    col("makt.MAKTX").alias("Material Text"),
    col("ekpo.TXZ01").alias("Short Text"),
    col("ekpo.IDNLF").alias("Vendor Material Number"),
    when(col("ekpo.RETPO") == "X", lit("Yes")).otherwise(lit("No")).alias("Return item"),
    when(col("ekpo.ELIKZ") == "X", lit("Yes")).otherwise(lit("No")).alias("Delivery Completed"),

    col("ekpo.MENGE").cast(DecimalType(15, 2)).alias("Ordered"),

    safe_div(col("eket.MENGE") * col("ekpo.UMREZ"), col("ekpo.UMREN"))
        .cast(DecimalType(15, 2)).alias("Rec. regd quantity"),

    when(col("ekpo.RETPO") == "X", lit(0).cast(DecimalType(15, 2)))
    .when(
        (col("ekpo.MENGE") - (coalesce(col("ekbe_gr.GR_MENGE"), lit(0)) +
                              coalesce(col("ekbe_gr.GR_WESBS"), lit(0)))) < 0,
        lit(0).cast(DecimalType(15, 2))
    )
    .otherwise(
        (col("ekpo.MENGE") - (coalesce(col("ekbe_gr.GR_MENGE"), lit(0)) +
                               coalesce(col("ekbe_gr.GR_WESBS"), lit(0))))
        .cast(DecimalType(15, 2))
    ).alias("To be delivered"),

    when(col("ekpo.RETPO") == "X", lit(0).cast(DecimalType(15, 2)))
    .when(
        (col("ekpo.MENGE") - (coalesce(col("ekbe_ir.IR_MENGE"), lit(0)) +
                              coalesce(col("ekbe_ir.IR_WESBS"), lit(0)))) < 0,
        lit(0).cast(DecimalType(15, 2))
    )
    .otherwise(
        (col("ekpo.MENGE") - (coalesce(col("ekbe_ir.IR_MENGE"), lit(0)) +
                               coalesce(col("ekbe_ir.IR_WESBS"), lit(0))))
        .cast(DecimalType(15, 2))
    ).alias("To be invoiced"),
    (col("ekpo.NETPR") * col("ekpo.MENGE")).cast(DecimalType(15, 2)).alias("Item value"),
    col("ekpo.NETWR").cast(DecimalType(15, 2)).alias("Net Order Value"),
    safe_div(col("ekpo.NETPR"), col("ekpo.PEINH")).cast(DecimalType(15, 2)).alias("Net Price"),
    col("ekpo.NETPR").cast(DecimalType(15, 2)).alias("Net Order Price"),
    col("mbew.STPRS").cast(DecimalType(15, 2)).alias("Standard price"),
    (
        safe_div(col("mbew.STPRS"), col("mbew.PEINH"))
        - (
            coalesce(col("mat_overhead_material.KBETR"), lit(0))
            / lit(10.0)
            * safe_div(col("ekpo.NETPR"), col("ekpo.PEINH") * col("ekpo.UMREZ"))
            / lit(100)
          )
        - (
            coalesce(col("freight_material.KBETR"), lit(0))
            / lit(10.0)
            * safe_div(col("ekpo.NETPR"), col("ekpo.PEINH") * col("ekpo.UMREZ"))
            / lit(100)
          )
    ) * safe_div(col("eket.MENGE") * col("ekpo.UMREZ"), col("ekpo.UMREN"))
    .alias("Net STD x QTY LC"),
    col("ekko.WAERS").alias("Currency"),
    col("mbew.PEINH").cast(DecimalType(15, 2)).alias("Price Unit Valuation"),
    col("ekpo.PEINH").cast(DecimalType(15, 2)).alias("Price Unit"),
    col("ekpo.BPRME").alias("Order Price Unit"),
    col("ekkn.VBELN").alias("Sales Document"),
    col("ekkn.VBELP").alias("Sales Document Item"),
    expr("""
        substring(ekes.VBELN,
            length(ekes.VBELN) - length(ltrim('0', ekes.VBELN)) + 1,
            length(ekes.VBELN))
    """).alias("Inb. Delivery"),

    col("likp.BOLNR").alias("Bill of lading"),
    col("ekpo.EVERS").alias("Shipping Instr."),
    col("rbkp.XBLNR").alias("Reference"),
    col("ekpo.BRGEW").cast(DecimalType(15, 2)).alias("Gross Weight"),
    col("ekpo.GEWEI").alias("Unit of Weight"),
    # col("ekko.ZZEXPD").alias("Express Delivery"),
    col("marc.MAABC").alias("ABC Indicator"),
    col("marc.STAWN").alias("Customs Tariff No"),
    col("eipo.STAWN").alias("OutB Tariff No"),

    col("ekpo.LABNR").alias("Order Acknowledgment"),
    col("ekes.XBLNR").alias("Conf. Reference"),
    sap_date(col("ekes.ERDAT")).alias("Conf. Date"),
    sap_date(col("eket.EINDT")).alias("Requested deliv. date"),
    sap_date(col("eket.EINDT")).alias("Delivery Date"),
    sap_date(col("eket.SLFDT")).alias("Stat. Rel. Del. Date"),
    sap_date(col("ekbe_gr.GR_BUDAT")).alias("GR date"),
    sap_date(col("ekbe_ir.IR_BUDAT")).alias("IR date"),
    sap_date(col("ekbe_gi.BUDAT")).alias("GI date"),

    sap_date(col("eket.EINDT")).alias("Planned dates"),

    lit("Order item schedule line").alias("Short Descript."),
    col("t001w.WERKS").alias("Production Plant"),
    col("marc.FEVOR").alias("Prodn Supervisor"),
    col("marc.BESKZ").alias("Procurement type"),
    lit("").alias("Fixed Indicator"),

    
    concat(col("ekko.EBELN"), col("ekpo.EBELP"), col("eket.ETENR")).alias("INTEGRATION_ID"),

    when(col("eket.WEMNG") != col("eket.MENGE"), lit("Partially Received"))
        .otherwise(lit("Fully Received")).alias("ReceiptStatus"),

    when(coalesce(col("ekbe_gr.GR_MENGE"), lit(0)) != col("ekpo.MENGE"), lit("Yes"))
        .otherwise(lit("No")).alias("QuantityMismatch"),
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

for c in final_df.columns:
    clean_name = c.strip().replace(" ", "_")
    final_df = final_df.withColumnRenamed(c, clean_name)

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

final_df = final_df.withColumnRenamed(
    "((((STPRS_/_CASE_WHEN_(PEINH_=_0)_THEN_NULL_ELSE_PEINH_END)_-_(((coalesce(KBETR,_0)_/_10.0)_*_(NETPR_/_CASE_WHEN_((PEINH_*_UMREZ)_=_0)_THEN_NULL_ELSE_(PEINH_*_UMREZ)_END))_/_100))_-_(((coalesce(KBETR,_0)_/_10.0)_*_(NETPR_/_CASE_WHEN_((PEINH_*_UMREZ)_=_0)_THEN_NULL_ELSE_(PEINH_*_UMREZ)_END))_/_100))_*_((MENGE_*_UMREZ)_/_CASE_WHEN_(UMREN_=_0)_THEN_NULL_ELSE_UMREN_END)_AS_`Net_STD_x_QTY_LC`)",
    "Net_STD_QTY_LC"
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


# final_df = final_df.filter(col("Purchasing_Document").like("95"))
# from pyspark.sql.functions import col

final_df1 = final_df.filter(col("Purchasing_Document").cast("string").like("95%"))

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

display(final_df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df1.write.mode("overwrite").saveAsTable("analytics.purchasing_document") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
