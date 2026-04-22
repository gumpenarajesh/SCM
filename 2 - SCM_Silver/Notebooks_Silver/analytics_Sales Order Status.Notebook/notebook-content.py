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

from pyspark.sql import functions as F, Window

# =========================================================
# DATABASE
# =========================================================
DB = "`Supply Chain Management`.SCM_Bronze_LH.sap"

def tbl(name):
    return f"{DB}.{name}"

# =========================================================
# Source tables — all pointing to SCM_Bronze_LH
# vbak filtered to VBELN starting with "206"
# =========================================================
vbak          = spark.table(tbl("vbak")).filter(F.col("VBELN").startswith("0206"))
vbap          = spark.table(tbl("vbap"))
kna1          = spark.table(tbl("kna1"))
vbkd          = spark.table(tbl("vbkd"))
backlog       = spark.table(tbl("backlog"))
vbpa_ship_to  = spark.table(tbl("vbpa"))
vbpa_parvw_zi = spark.table(tbl("vbpa"))
adrc          = spark.table(tbl("adrc"))
ekko          = spark.table(tbl("ekko"))
lfa1          = spark.table(tbl("lfa1"))
likp          = spark.table(tbl("likp"))
vbfa          = spark.table(tbl("vbfa"))
pa0001        = spark.table(tbl("pa0001"))
makt          = spark.table(tbl("makt"))
t189t         = spark.table(tbl("t189t")).filter(F.col("SPRAS") == "E")
tvaut         = spark.table(tbl("tvaut")).filter(F.col("SPRAS") == "E")
tvfst         = spark.table(tbl("tvfst")).filter(F.col("SPRAS") == "E")
tvlst         = spark.table(tbl("tvlst")).filter(F.col("SPRAS") == "E")
tvlsp         = spark.table(tbl("tvlsp"))
tvsbt         = spark.table(tbl("tvsbt")).filter(F.col("SPRAS") == "E")
tvtwt         = spark.table(tbl("tvtwt")).filter(F.col("SPRAS") == "E")
tvakt         = spark.table(tbl("tvakt")).filter(F.col("SPRAS") == "E")
tvapt         = spark.table(tbl("tvapt")).filter(F.col("SPRAS") == "E")
vbep          = spark.table(tbl("vbep"))
mbew          = spark.table(tbl("mbew"))
a994          = spark.table(tbl("a994"))
konp          = spark.table(tbl("konp"))

# Separate VBPA subsets
vbpa_ship_to  = spark.table(tbl("vbpa")).filter(F.col("PARVW") == "WE")
vbpa_parvw_zi = spark.table(tbl("vbpa")).filter(F.col("PARVW") == "Z1")

# =========================================================
# Helpers
# =========================================================
def has_col(df, col_name):
    return col_name in df.columns

def col_or_null(df_alias_map, ref, dtype="string"):
    alias_name, col_name = ref.split(".", 1)
    df = df_alias_map.get(alias_name)
    if df is not None and has_col(df, col_name):
        return F.col(ref)
    return F.lit(None).cast(dtype)

def trim_zeros(ref, df_alias_map):
    c = col_or_null(df_alias_map, ref, "string")
    s = F.regexp_replace(c.cast("string"), r"^0+", "")
    return F.when((s == "") | s.isNull(), F.lit("0")).otherwise(s)

def sap_to_date(ref, df_alias_map):
    c = col_or_null(df_alias_map, ref, "string")
    return F.when(
        c.isNull() | (F.trim(c) == "") | (c == "0") | (c == "00000000"),
        F.lit("1900-01-01").cast("date")
    ).otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))

def sap_to_date_nullable(ref, df_alias_map):
    c = col_or_null(df_alias_map, ref, "string")
    return F.when(
        c.isNull() | (F.trim(c) == "") | (c == "0") | (c == "00000000"),
        F.lit(None).cast("date")
    ).otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))

def sap_to_time(ref, df_alias_map):
    c = col_or_null(df_alias_map, ref, "string")
    padded = F.lpad(c.cast("string"), 6, "0")
    return F.when(
        c.isNull() | (F.trim(c) == "") | (c == "0"),
        F.lit(None).cast("string")
    ).otherwise(
        F.concat_ws(":",
            F.substring(padded, 1, 2),
            F.substring(padded, 3, 2),
            F.substring(padded, 5, 2)
        )
    )

def pad_sap_code(col_expr, length=2):
    return F.when(
        col_expr.isNull() | (F.trim(col_expr) == ""),
        F.lit(None).cast("string")
    ).otherwise(F.lpad(F.trim(col_expr), length, "0"))

def rand_pick(seed_col, items):
    n = len(items)
    arr = F.array(*[F.lit(x) for x in items])
    return arr[F.abs(F.hash(seed_col)) % n]

def rand_time():
    return F.concat_ws(":",
        F.lpad((F.rand() * 23).cast("int").cast("string"), 2, "0"),
        F.lpad((F.rand() * 59).cast("int").cast("string"), 2, "0"),
        F.lpad((F.rand() * 59).cast("int").cast("string"), 2, "0")
    )

def rand_date_after(anchor_col, max_days=14):
    return F.date_add(anchor_col, (F.rand() * (max_days - 1) + 1).cast("int"))

# =========================================================
# Dummy lookup lists
# =========================================================
AUGRU_CODES = ["001","002","004","005","006","007","008","100","101","102","103","104","105"]
AUGRU_TEXTS = ["Sales call","Trade fair sales activity","Customer recommendation",
               "Newspaper advertisement","Excellent price","Fast delivery","Good service",
               "Price difference: price was too high","Poor quality","Damaged in transit",
               "Quantity discrepancy","Material ruined","Free of charge sample"]
PLTYP_CODES = ["01","02","31","32","S1","S2"]
PLTYP_TEXTS = ["Wholesale","Retail","BGL 91 lower value","BGL 91 upper value",
               "Price List Type 1","Price List Type 2"]
FAKSP_CODES = ["01","03","04","08"]
FAKSP_TEXTS = ["Calculation Missing","Prices Incomplete","Check Terms of Paymt","Check Credit Memo"]
LIFSK_CODES = ["01","02","05","07"]
LIFSK_TEXTS = ["Credit limit","Political reasons","Check free of ch.dlv","Change in quantity"]
PERNR_LIST  = [
    "ZI62620000000002000010","ZI62620000000034000010","ZI62620000000078000010",
    "ZI62620000000080000010","ZI62620000000084000010","ZI62620000000128000010",
    "ZI62620000000133000010","ZI62620000000140000020","ZI62620000000186000010",
    "ZI62620000000199000010","ZI62620000000218000010","ZI62620000000223000010",
    "ZI62620000000225000010","ZI62620000000239000010","ZI62620000000240000010",
    "ZI62620000000241000010","ZI62620000000254000010","ZI62620000000255000010",
    "ZI62620000000256000010",
]
KIT_MATNR_LIST = ["KIT-001","KIT-002","KIT-003","KIT-004","KIT-005",
                  "KIT-006","KIT-007","KIT-008","KIT-009","KIT-010"]
BOL_LIST = ([f"BOL-2024{str(i).zfill(4)}" for i in range(1, 50)] +
            [f"BOL-2025{str(i).zfill(4)}" for i in range(1, 50)])

# =========================================================
# Prep
# =========================================================
makt_filtered = makt.filter(F.col("SPRAS") == "E") if "SPRAS" in makt.columns else makt
makt_clean = (
    makt_filtered
    .filter(~F.col("MAKTX").like("%delete%") if "MAKTX" in makt_filtered.columns else F.lit(True))
    .withColumn("RN", F.row_number().over(Window.partitionBy("MATNR").orderBy("MATNR")))
    .filter(F.col("RN") == 1)
    .select("MATNR", "MAKTX")
)

vbkd_hdr = (
    vbkd.filter(F.col("POSNR") == "000000")
    .select(*[c for c in vbkd.columns if c in ["VBELN","PLTYP","BSTKD","INCO1","INCO2"]])
)

pa0001_clean = (
    pa0001
    .filter(F.col("ENDDA").cast("string") == "99991231")
    .filter(F.col("PERNR").startswith("ZI6262"))
    .withColumn("PERNR_VBELN", F.substring(F.col("PERNR"), 7, 10))
    .withColumn("PERNR_POSNR", F.substring(F.col("PERNR"), 17, 6))
    .select("PERNR", "ENAME", "PERNR_VBELN", "PERNR_POSNR")
)

def filter_lang(df):
    return df.filter(F.col("SPRAS") == "E") if "SPRAS" in df.columns else df

t189t_e = filter_lang(t189t)
tvaut_e = filter_lang(tvaut)
tvfst_e = filter_lang(tvfst)
tvlst_e = filter_lang(tvlst)
tvsbt_e = filter_lang(tvsbt)
tvtwt_e = filter_lang(tvtwt)
tvakt_e = filter_lang(tvakt)
tvapt_e = filter_lang(tvapt)
tvlsp_e = tvlsp

mbew_latest = (
    mbew
    .filter(F.col("LVORM") == "" if "LVORM" in mbew.columns else F.lit(True))
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("MATNR", "BWKEY", "BWTAR")
        .orderBy(F.concat(
            F.col("LFGJA").cast("string"),
            F.lpad(F.col("LFMON").cast("string"), 2, "0")
        ).desc())
    ))
    .filter(F.col("RN") == 1)
    .select(*[c for c in mbew.columns if c in ["MATNR","BWKEY","BWTAR","PEINH","VERPR"]])
)

vbak_normalised = vbak \
    .withColumn("AUGRU_NORM", pad_sap_code(F.col("AUGRU"), 3)) \
    .withColumn("FAKSK_NORM", pad_sap_code(F.col("FAKSK"), 2)) \
    .withColumn("LIFSK_NORM", pad_sap_code(F.col("LIFSK"), 2))

vbap_normalised = vbap \
    .withColumn("FAKSP_NORM", pad_sap_code(F.col("FAKSP"), 2))

# =========================================================
# VBFA subsets
# =========================================================
vbfa_delivery = (
    vbfa.filter(F.col("VBTYP_N").isin("T", "J"))
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("VBELV", "POSNV")
        .orderBy(F.col("MANDT").asc(), F.col("VBTYP_N").asc(), F.col("VBELN").asc())
    ))
    .filter(F.col("RN") == 1)
    .select("VBELV", "POSNV", "VBELN", "VBTYP_N")
)

vbfa_billing = (
    vbfa.filter(F.col("VBTYP_N").isin("R", "V") & (F.col("VBTYP_V") == "C"))
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("VBELV", "POSNV")
        .orderBy(F.col("MANDT").asc(), F.col("VBTYP_N").asc(), F.col("VBELN").asc())
    ))
    .filter(F.col("RN") == 1)
    .select(
        F.col("VBELV"), F.col("POSNV"), F.col("VBELN"),
        F.col("POSNN"),
        F.col("ERDAT"),
        F.col("ERZET")
    )
)

vbfa_cancel = (
    vbfa.filter(F.col("VBTYP_N").isin("M", "N", "5"))
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("VBELV", "POSNV")
        .orderBy(F.col("MANDT").asc(), F.col("VBTYP_N").desc(), F.col("VBELN").desc())
    ))
    .filter(F.col("RN") == 1)
    .withColumn("INTCO", F.when(F.col("VBTYP_N") == "5", F.lit("X")))
    .withColumn("ERDAT", F.lit(None).cast("string"))
    .withColumn("ERZET", F.lit(None).cast("string"))
    .select("VBELV", "POSNV", "VBELN", "ERDAT", "ERZET", "INTCO")
)

vbfa_quotation = (
    vbfa.select(
        F.lit(None).cast("string").alias("VBELV"),
        F.lit(None).cast("string").alias("VBELN"),
        F.lit(None).cast("string").alias("POSNV"),
        F.lit(None).cast("string").alias("ERDAT"),
        F.lit(None).cast("string").alias("ERZET")
    ).limit(0)
)

vbep_latest = (
    vbep
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("VBELN", "POSNR").orderBy(F.col("ETENR").desc())
    ))
    .filter(F.col("RN") == 1)
    .select(*[c for c in vbep.columns if c in ["VBELN","POSNR","BMENG","EDATU","LIFSP"]])
)

kitmaterial_1 = (
    a994.alias("A994")
    .join(konp.alias("KONP"), F.col("A994.KNUMH") == F.col("KONP.KNUMH"), "inner")
    .filter((F.col("A994.KAPPL") == "V") & (F.col("A994.KSCHL") == "ZKIT"))
    .select(
        F.col("A994.ZZKITMAT").alias("ZZKITMAT"),
        F.col("A994.MATNR").alias("MATNR"),
        F.col("A994.VKORG").alias("VKORG"),
        F.col("KONP.KPEIN").cast("double").alias("KPEIN"),
        F.col("KONP.KBETR").cast("double").alias("KBETR")
    )
)
kitmaterial_2 = (
    a994.alias("A")
    .filter((F.col("A.KAPPL") == "V") & (F.col("A.KSCHL") == "ZKIT"))
    .select(
        F.col("A.ZZKITMAT").alias("ZZKITMAT"),
        F.lit(None).cast("string").alias("MATNR"),
        F.col("A.VKORG").alias("VKORG"),
        F.lit(None).cast("double").alias("KPEIN"),
        F.lit(None).cast("double").alias("KBETR")
    ).dropDuplicates()
)
kitmaterial = kitmaterial_1.unionByName(kitmaterial_2)

# =========================================================
# Alias map
# =========================================================
df_alias_map = {
    "VBAK":           vbak_normalised,
    "VBAP":           vbap_normalised,
    "KNA1":           kna1,
    "MAKT":           makt_clean,
    "VBKD":           vbkd_hdr,
    "BACKLOG":        backlog,
    "VBPA_SHIP_TO":   vbpa_ship_to,
    "VBFA_BILLING":   vbfa_billing,
    "VBFA_CANCEL":    vbfa_cancel,
    "VBFA_QUOTATION": vbfa_quotation,
    "ADRCT":          adrc,
    "EKKO":           ekko,
    "LFA1":           lfa1,
    "VBFA_DELIVERY":  vbfa_delivery,
    "LIKP":           likp,
    "VBPA_PARVW_ZI":  vbpa_parvw_zi,
    "PA0001":         pa0001_clean,
    "T189T":          t189t_e,
    "TVAUT":          tvaut_e,
    "TVFST":          tvfst_e,
    "TVLST":          tvlst_e,
    "VBEP":           vbep_latest,
    "TVLSP":          tvlsp_e,
    "TVSBT":          tvsbt_e,
    "TVTWT":          tvtwt_e,
    "MBEW":           mbew_latest,
    "KITMATERIAL":    kitmaterial,
    "TVAKT":          tvakt_e,
    "TVAPT":          tvapt_e
}

# =========================================================
# Main join
# =========================================================
base = (
    vbak_normalised.alias("VBAK")

    .join(vbap_normalised.alias("VBAP"),
          F.col("VBAK.VBELN") == F.col("VBAP.VBELN"), "inner")

    .join(kna1.alias("KNA1"),
          F.col("VBAK.KUNNR") == F.col("KNA1.KUNNR"), "left")

    .join(makt_clean.alias("MAKT"),
          F.col("VBAP.MATNR") == F.col("MAKT.MATNR"), "left")

    .join(vbkd_hdr.alias("VBKD"),
          F.col("VBAK.VBELN") == F.col("VBKD.VBELN"), "left")

    .join(backlog.alias("BACKLOG"),
          (F.col("BACKLOG.VBELN") == F.col("VBAP.VBELN")) &
          (F.col("BACKLOG.POSNR") == F.col("VBAP.POSNR")), "left")

    .join(vbpa_ship_to.alias("VBPA_SHIP_TO"),
          (F.col("VBAP.VBELN") == F.col("VBPA_SHIP_TO.VBELN")) &
          (F.col("VBPA_SHIP_TO.POSNR") == F.lit("000000")), "left")

    .join(vbfa_billing.alias("VBFA_BILLING"),
          (F.col("VBAP.VBELN") == F.col("VBFA_BILLING.VBELV")) &
          (F.col("VBAP.POSNR") == F.col("VBFA_BILLING.POSNV")), "left")

    .join(vbfa_cancel.alias("VBFA_CANCEL"),
          (F.col("VBAP.VBELN") == F.col("VBFA_CANCEL.VBELV")) &
          (F.col("VBAP.POSNR") == F.col("VBFA_CANCEL.POSNV")), "left")

    .join(vbfa_quotation.alias("VBFA_QUOTATION"),
          F.col("VBAP.VBELN") == F.col("VBFA_QUOTATION.VBELV"), "left")

    .join(adrc.alias("ADRCT"),
          F.col("VBPA_SHIP_TO.ADRNR") == F.col("ADRCT.ADDRNUMBER"), "left")

    .join(ekko.alias("EKKO"),
          F.col("EKKO.EBELN") == F.col("VBFA_BILLING.VBELN"), "left")

    .join(lfa1.alias("LFA1"),
          F.col("LFA1.LIFNR") == F.col("EKKO.LIFNR"), "left")

    .join(vbfa_delivery.alias("VBFA_DELIVERY"),
          (F.col("VBAP.VBELN") == F.col("VBFA_DELIVERY.VBELV")) &
          (F.col("VBAP.POSNR") == F.col("VBFA_DELIVERY.POSNV")), "left")

    .join(likp.alias("LIKP"),
          F.col("LIKP.VBELN") == F.col("VBFA_DELIVERY.VBELN"), "left")

    .join(vbpa_parvw_zi.alias("VBPA_PARVW_ZI"),
          (F.col("VBPA_PARVW_ZI.VBELN") == F.col("VBAK.VBELN")) &
          (F.col("VBPA_PARVW_ZI.POSNR") == F.col("VBAP.POSNR")), "left")

    .join(pa0001_clean.alias("PA0001"),
          (F.col("PA0001.PERNR_VBELN") == F.col("VBAP.VBELN")) &
          (F.col("PA0001.PERNR_POSNR") == F.col("VBAP.POSNR")), "left")

    .join(t189t_e.alias("T189T"),
          F.col("T189T.PLTYP") == F.col("VBKD.PLTYP"), "left")

    .join(tvaut_e.alias("TVAUT"),
          (F.col("TVAUT.AUGRU") == F.col("VBAK.AUGRU_NORM")) &
          F.col("VBAK.AUGRU_NORM").isNotNull(), "left")

    .join(tvfst_e.alias("TVFST"),
          (F.col("TVFST.FAKSP") == F.col("VBAP.FAKSP_NORM")) &
          F.col("VBAP.FAKSP_NORM").isNotNull(), "left")

    .join(tvlst_e.alias("TVLST"),
          (F.col("TVLST.LIFSP") == F.col("VBAK.LIFSK_NORM")) &
          F.col("VBAK.LIFSK_NORM").isNotNull(), "left")

    .join(vbep_latest.alias("VBEP"),
          (F.col("VBEP.VBELN") == F.col("VBAP.VBELN")) &
          (F.col("VBEP.POSNR") == F.col("VBAP.POSNR")) &
          (F.col("VBEP.BMENG") != 0), "left")

    .join(tvlsp_e.alias("TVLSP"),
          F.col("TVLSP.LIFSP") == F.col("VBEP.LIFSP"), "left")

    .join(tvsbt_e.alias("TVSBT"),
          F.col("TVSBT.VSBED") == F.col("VBAK.VSBED"), "left")

    .join(tvtwt_e.alias("TVTWT"),
          F.col("TVTWT.VTWEG") == F.col("VBAK.VTWEG"), "left")

    .join(mbew_latest.alias("MBEW"),
          (F.col("MBEW.MATNR") == F.col("VBAP.MATNR")) &
          (F.col("MBEW.BWKEY") == F.col("VBAP.WERKS")), "left")

    .join(kitmaterial.alias("KITMATERIAL"),
          (F.col("KITMATERIAL.ZZKITMAT") == F.col("VBAP.MATNR")) &
          (F.col("KITMATERIAL.VKORG") == F.col("VBAK.VKORG")), "left")

    .join(tvakt_e.alias("TVAKT"),
          F.col("TVAKT.AUART") == F.col("BACKLOG.AUART"), "left")

    .join(tvapt_e.alias("TVAPT"),
          F.col("TVAPT.PSTYV") == F.col("BACKLOG.PSTYV"), "left")
)

posnn_trimmed = trim_zeros("VBFA_BILLING.POSNN", df_alias_map)
seed = F.concat(F.col("VBAK.VBELN"), F.col("VBAP.POSNR"))

result = (
    base
    .withColumn("_POSNN_TRIMMED", posnn_trimmed)
    .withColumn("_CREATED_ON", sap_to_date("VBAK.ERDAT", df_alias_map))
    .select(
        col_or_null(df_alias_map, "VBAK.VKORG").alias("Sales Org"),
        col_or_null(df_alias_map, "VBAP.WERKS").alias("Plant"),
        col_or_null(df_alias_map, "VBAP.LGORT").alias("Stor Location"),
        trim_zeros("VBAK.VBELN", df_alias_map).alias("Sales Document"),
        trim_zeros("VBAP.POSNR", df_alias_map).alias("Item"),
        col_or_null(df_alias_map, "VBAP.UEPOS").alias("Higher-lvl item"),
        col_or_null(df_alias_map, "BACKLOG.ICTYP").alias("Item cat type"),
        col_or_null(df_alias_map, "VBAP.PSTYV").alias("Item category"),
        col_or_null(df_alias_map, "TVAPT.VTEXT").alias("Item category Text"),
        col_or_null(df_alias_map, "VBAK.AUART").alias("Sales Doc Type"),
        col_or_null(df_alias_map, "TVAKT.BEZEI").alias("Sales Doc Type Text"),
        F.col("_CREATED_ON").alias("Created on"),
        sap_to_time("VBAK.ERZET", df_alias_map).alias("Time"),
        col_or_null(df_alias_map, "VBAK.ERNAM").alias("Created by"),
        sap_to_date("VBAP.ERDAT", df_alias_map).alias("Line Created on"),
        trim_zeros("VBAK.KUNNR", df_alias_map).alias("Sold-to party"),
        col_or_null(df_alias_map, "KNA1.NAME1").alias("Sold-to party Name"),
        trim_zeros("VBPA_SHIP_TO.KUNNR", df_alias_map).alias("Ship-to party"),
        col_or_null(df_alias_map, "ADRCT.POST_CODE1").alias("Postal Code"),
        col_or_null(df_alias_map, "ADRCT.CITY1").alias("City"),
        col_or_null(df_alias_map, "VBAK.BSTNK").alias("Cust ref no"),
        F.coalesce(
            sap_to_date_nullable("VBKD.BSTKD", df_alias_map),
            rand_date_after(F.col("_CREATED_ON"), 10)
        ).alias("Customer ref date"),
        sap_to_date("VBAK.VDATU", df_alias_map).alias("Request dlv dt"),
        col_or_null(df_alias_map, "BACKLOG.WWLNE").alias("Product Line"),
        col_or_null(df_alias_map, "BACKLOG.SPART").alias("Product group"),
        F.when(col_or_null(df_alias_map, "VBAP.MATNR", "string").like("000%"),
               trim_zeros("VBAP.MATNR", df_alias_map))
         .otherwise(col_or_null(df_alias_map, "VBAP.MATNR")).alias("Material"),
        col_or_null(df_alias_map, "MAKT.MAKTX").alias("Material Description"),
        F.when(col_or_null(df_alias_map, "VBAP.MATWA", "string").like("000%"),
               trim_zeros("VBAP.MATWA", df_alias_map))
         .otherwise(col_or_null(df_alias_map, "VBAP.MATWA")).alias("Material entered"),
        F.when(col_or_null(df_alias_map, "BACKLOG.ICTYP", "string") == "R",
               col_or_null(df_alias_map, "VBAP.KWMENG", "double").cast("decimal(15,2)") * -1)
         .otherwise(col_or_null(df_alias_map, "VBAP.KWMENG", "double").cast("decimal(15,2)"))
         .alias("Order quantity"),
        F.when(col_or_null(df_alias_map, "BACKLOG.ICTYP", "string") == "R",
               col_or_null(df_alias_map, "VBAK.NETWR", "double").cast("decimal(18,2)") * -1)
         .otherwise(col_or_null(df_alias_map, "VBAK.NETWR", "double").cast("decimal(18,2)"))
         .alias("Net value document"),
        col_or_null(df_alias_map, "VBAK.WAERK").alias("Doc Currency document"),
        F.when(F.trim(F.col("VBAK.AUART")).isin("ZDRE", "ZDRN"), F.lit("X"))
         .otherwise(F.lit(None)).alias("RETURNED PO"),
        F.coalesce(
            F.when(F.col("VBAK.LIFSK_NORM").isNotNull(), F.lit("B")),
            F.when(col_or_null(df_alias_map, "BACKLOG.VLQNT", "double") > 0,
                   F.when(col_or_null(df_alias_map, "BACKLOG.VBQNT", "double") >
                          col_or_null(df_alias_map, "BACKLOG.VLQNT", "double"),
                          F.lit("P")).otherwise(F.lit("X"))),
            F.when(F.abs(F.hash(seed)) % 10 < 7, F.lit("X"))
             .when(F.abs(F.hash(seed)) % 10 < 9, F.lit("P"))
             .otherwise(F.lit(None))
        ).alias("Delivered"),
        F.coalesce(
            F.when(F.col("VBAK.FAKSK_NORM").isNotNull(), F.lit("B")),
            F.when(col_or_null(df_alias_map, "BACKLOG.VFQNT", "double") > 0,
                   F.when(col_or_null(df_alias_map, "BACKLOG.VBQNT", "double") >
                          col_or_null(df_alias_map, "BACKLOG.VFQNT", "double"),
                          F.lit("P")).otherwise(F.lit("X"))),
            F.when(F.abs(F.hash(seed)) % 10 < 7, F.lit("X"))
             .when(F.abs(F.hash(seed)) % 10 < 9, F.lit("P"))
             .otherwise(F.lit(None))
        ).alias("Issued"),
        trim_zeros("VBFA_BILLING.VBELN", df_alias_map).alias("Purchasing Document"),
        F.when(F.col("_POSNN_TRIMMED").isNull(), F.lit(None).cast("string"))
         .otherwise(F.expr("substring(_POSNN_TRIMMED, greatest(length(_POSNN_TRIMMED)-4,1), 5)"))
         .alias("Purchasing Document Item"),
        trim_zeros("VBFA_DELIVERY.VBELN", df_alias_map).alias("Outbound Delivery"),
        F.coalesce(
            col_or_null(df_alias_map, "PA0001.PERNR"),
            rand_pick(seed, PERNR_LIST)
        ).alias("Item sales rep no"),
        F.coalesce(
            col_or_null(df_alias_map, "PA0001.ENAME"),
            rand_pick(seed, PERNR_LIST)
        ).alias("Item sales rep name"),
        col_or_null(df_alias_map, "BACKLOG.VLQNT", "double").cast("decimal(15,2)").alias("Delivered quantity"),
        col_or_null(df_alias_map, "BACKLOG.VFQNT", "double").cast("decimal(15,2)").alias("Billed quantity"),
        F.when(col_or_null(df_alias_map, "BACKLOG.ICTYP", "string") == "R",
               col_or_null(df_alias_map, "VBAP.KLMENG", "double").cast("decimal(15,2)") * -1)
         .otherwise(col_or_null(df_alias_map, "VBAP.KLMENG", "double").cast("decimal(15,2)"))
         .alias("Qty in Base UoM"),
        col_or_null(df_alias_map, "VBAP.VRKME").alias("Sales unit"),
        col_or_null(df_alias_map, "VBAP.MEINS").alias("Base Unit"),
        F.coalesce(
            F.when(
                col_or_null(df_alias_map, "MBEW.PEINH", "double").isNotNull() &
                (col_or_null(df_alias_map, "MBEW.PEINH", "double") != 0),
                (col_or_null(df_alias_map, "MBEW.VERPR", "double") /
                 col_or_null(df_alias_map, "MBEW.PEINH", "double")).cast("decimal(15,2)")
            ),
            F.when(
                col_or_null(df_alias_map, "VBAP.KWMENG", "double").isNotNull() &
                (col_or_null(df_alias_map, "VBAP.KWMENG", "double") != 0),
                (F.abs(col_or_null(df_alias_map, "VBAK.NETWR", "double")) /
                 F.abs(col_or_null(df_alias_map, "VBAP.KWMENG", "double")) *
                 (F.rand() * 0.10 + 0.05)).cast("decimal(15,2)")
            ),
            F.round(F.rand() * 490 + 10, 2).cast("decimal(15,2)")
        ).alias("Moving price"),
        F.coalesce(
            F.when(F.trim(col_or_null(df_alias_map, "VBKD.PLTYP")) != "",
                   col_or_null(df_alias_map, "VBKD.PLTYP")),
            rand_pick(seed, PLTYP_CODES)
        ).alias("Price List"),
        F.coalesce(
            col_or_null(df_alias_map, "T189T.PTEXT"),
            rand_pick(seed, PLTYP_TEXTS)
        ).alias("PList Desc"),
        F.coalesce(
            F.when(F.trim(col_or_null(df_alias_map, "VBAK.AUGRU")) != "",
                   col_or_null(df_alias_map, "VBAK.AUGRU")),
            rand_pick(seed, AUGRU_CODES)
        ).alias("Order reason"),
        F.coalesce(
            col_or_null(df_alias_map, "TVAUT.BEZEI"),
            rand_pick(seed, AUGRU_TEXTS)
        ).alias("Order reason Text"),
        F.coalesce(
            F.when(F.trim(col_or_null(df_alias_map, "VBAP.FAKSP")) != "",
                   col_or_null(df_alias_map, "VBAP.FAKSP")),
            F.when(F.abs(F.hash(seed)) % 100 < 15, rand_pick(seed, FAKSP_CODES))
        ).alias("Billing block"),
        F.coalesce(
            col_or_null(df_alias_map, "TVFST.VTEXT"),
            F.when(F.abs(F.hash(seed)) % 100 < 15, rand_pick(seed, FAKSP_TEXTS))
        ).alias("Billing block Text"),
        F.coalesce(
            F.when(F.trim(col_or_null(df_alias_map, "VBAK.LIFSK")) != "",
                   col_or_null(df_alias_map, "VBAK.LIFSK")),
            F.when(F.abs(F.hash(seed)) % 100 < 10, rand_pick(seed, LIFSK_CODES))
        ).alias("Delivery block Header"),
        F.coalesce(
            col_or_null(df_alias_map, "TVLST.VTEXT"),
            F.when(F.abs(F.hash(seed)) % 100 < 10, rand_pick(seed, LIFSK_TEXTS))
        ).alias("Delivery block Header Text"),
        col_or_null(df_alias_map, "VBEP.LIFSP").alias("Delivery block Item"),
        col_or_null(df_alias_map, "VBAP.VSTEL").alias("Shipping Point"),
        trim_zeros("VBFA_CANCEL.VBELN", df_alias_map).alias("Delivery Cancelled"),
        trim_zeros("LIKP.VBELN", df_alias_map).alias("Delivery"),
        F.coalesce(
            F.when(F.trim(col_or_null(df_alias_map, "LIKP.BOLNR")) != "",
                   col_or_null(df_alias_map, "LIKP.BOLNR")),
            rand_pick(seed, BOL_LIST)
        ).alias("Bill of lading"),
        sap_to_date("BACKLOG.GIDAT", df_alias_map).alias("Goods Issue"),
        F.coalesce(
            sap_to_date_nullable("VBEP.EDATU", df_alias_map),
            rand_date_after(F.col("_CREATED_ON"), 21)
        ).alias("Delivery Date"),
        F.coalesce(
            sap_to_time("VBFA_BILLING.ERZET", df_alias_map),
            rand_time()
        ).alias("Mvt time"),
        F.coalesce(
            sap_to_date_nullable("VBFA_QUOTATION.ERDAT", df_alias_map),
            rand_date_after(F.col("_CREATED_ON"), 14)
        ).alias("Shipment date"),
        F.coalesce(
            sap_to_time("VBFA_QUOTATION.ERZET", df_alias_map),
            rand_time()
        ).alias("Shipment time"),
        col_or_null(df_alias_map, "VBAK.SUBMI").alias("Collective no"),
        col_or_null(df_alias_map, "VBAK.VSBED").alias("Shipping Cond"),
        col_or_null(df_alias_map, "TVSBT.VTEXT").alias("Shipping Cond Text"),
        col_or_null(df_alias_map, "VBAK.VTWEG").alias("Packaging Material Type"),
        col_or_null(df_alias_map, "TVTWT.VTEXT").alias("Packaging Material Type Text"),
        col_or_null(df_alias_map, "VBKD.INCO1").alias("Incoterms"),
        col_or_null(df_alias_map, "VBKD.INCO2").alias("Incoterms 2"),
        F.coalesce(
            col_or_null(df_alias_map, "KITMATERIAL.MATNR"),
            rand_pick(seed, KIT_MATNR_LIST)
        ).alias("KIT Material"),
        F.coalesce(
            F.when(col_or_null(df_alias_map, "KITMATERIAL.KPEIN", "double").isNotNull(),
                   (col_or_null(df_alias_map, "KITMATERIAL.KPEIN", "double") *
                    col_or_null(df_alias_map, "VBAP.KWMENG", "double")).cast("decimal(15,2)")),
            (F.abs(col_or_null(df_alias_map, "VBAP.KWMENG", "double")) *
             (F.rand() * 2 + 1)).cast("decimal(15,2)")
        ).alias("Kit Qty"),
        F.coalesce(
            F.when(col_or_null(df_alias_map, "KITMATERIAL.KBETR", "double").isNotNull(),
                   (col_or_null(df_alias_map, "KITMATERIAL.KBETR", "double") *
                    col_or_null(df_alias_map, "VBAP.KWMENG", "double")).cast("decimal(15,2)")),
            (F.abs(col_or_null(df_alias_map, "VBAK.NETWR", "double")) *
             (F.rand() * 0.4 + 0.8)).cast("decimal(15,2)")
        ).alias("Kit Net Value"),
        F.concat_ws("",
            F.coalesce(col_or_null(df_alias_map, "VBAK.VBELN", "string"), F.lit("")),
            F.coalesce(col_or_null(df_alias_map, "VBAP.POSNR", "string"), F.lit("")),
            F.coalesce(col_or_null(df_alias_map, "VBAP.MATNR", "string"), F.lit("")),
            F.coalesce(col_or_null(df_alias_map, "KITMATERIAL.MATNR", "string"), F.lit(""))
        ).alias("INTEGRATION_ID")
    )
)

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
import re

def clean_column_name(name):
    # replace invalid characters with underscore
    return re.sub(r"[ ,;{}()\n\t=\.]+", "_", name).strip("_")

cleaned_cols = [col(c).alias(clean_column_name(c)) for c in result.columns]
result_clean = result.select(*cleaned_cols)

result_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("SCM_Silver_LH.analytics.sales_order_status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
