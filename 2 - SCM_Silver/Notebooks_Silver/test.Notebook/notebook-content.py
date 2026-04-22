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

# =============================================================================
# Current Stock – PySpark (Fabric / Databricks Notebook)
# Source:  SCM_Bronze_LH.sap.*
# Filters: Plant IN ('1710', 'DPSF', 'TA88')
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window  import Window

# ─────────────────────────────────────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────────────────────────────────────
WAREHOUSE = "SCM_Bronze_LH"
SCHEMA    = "sap"
PLANTS    = ["1710", "DPSF", "TA88"]   # ← plant filter

# ─────────────────────────────────────────────────────────────────────────────
# 1. Load source tables
# ─────────────────────────────────────────────────────────────────────────────
s032  = spark.table(f"{WAREHOUSE}.{SCHEMA}.S032")
mara  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARA")
makt  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MAKT")
mard  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARD")
mbew  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MBEW_1604")
marc  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARC")
t001w = spark.table(f"{WAREHOUSE}.{SCHEMA}.T001W")
mvke  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MVKE")

# Push plant filter as early as possible for partition pruning / performance
s032 = s032.filter(F.col("WERKS").isin(PLANTS))
mard = mard.filter(F.col("WERKS").isin(PLANTS))
marc = marc.filter(F.col("WERKS").isin(PLANTS))

# ─────────────────────────────────────────────────────────────────────────────
# 2. CTE: materialdescription
#    • Cross-join MARA ↔ MAKT (exclude descriptions containing "delete")
#    • Deduplicate with DISTINCT, then keep rownum = 1 per MATNR
# ─────────────────────────────────────────────────────────────────────────────
mat_desc_raw = (
    mara.select("MATNR")
        .join(
            makt.filter(~F.lower(F.col("MAKTX")).like("%delete%"))
                .select("MATNR", "MAKTX"),
            on="MATNR",
            how="left",
        )
        .select(
            F.col("MATNR"),
            F.col("MAKTX").alias("Material_Text"),
        )
        .distinct()
)

w_matdesc = Window.partitionBy("MATNR").orderBy("MATNR")

mat_desc = (
    mat_desc_raw
    .withColumn("rownum", F.row_number().over(w_matdesc))
    .filter(F.col("rownum") == 1)
    .drop("rownum")
)

# ─────────────────────────────────────────────────────────────────────────────
# 3. CTE: fp  (latest fiscal-period record per MATNR / BWKEY / BWTAR)
#    Mirrors: ROW_NUMBER OVER (PARTITION BY … ORDER BY CONCAT(LFGJA,LFMON) DESC)
# ─────────────────────────────────────────────────────────────────────────────
w_fp = (
    Window
    .partitionBy("MATNR", "BWKEY", "BWTAR")
    .orderBy(F.concat(F.col("LFGJA"), F.col("LFMON")).desc())
)

fp = (
    mbew
    .filter(F.col("LVORM") == "")
    .withColumn("fp_rownumber", F.row_number().over(w_fp))
    .filter(F.col("fp_rownumber") == 1)
    .select(
        "MATNR", "BWKEY", "BWTAR",
        "BKLAS", "VPRSV", "PEINH",
        "VERPR", "STPRS", "LBKUM",
        "SALK3", "LVORM",
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. CTE: procurement  (MAX(BESKZ) per MATNR / WERKS from active MARC rows)
# ─────────────────────────────────────────────────────────────────────────────
procurement = (
    marc
    .filter(F.col("LVORM") == "")
    .groupBy("MATNR", "WERKS")
    .agg(F.max("BESKZ").alias("BESKZ"))
)

# ─────────────────────────────────────────────────────────────────────────────
# 5. Helper: SAP numeric date (stored as integer YYYYMMDD) → DateType
#    Mirrors: CASE WHEN col = 0 THEN NULL
#             ELSE CAST(CONVERT(CHAR(8), col, 112) AS DATE) END
# ─────────────────────────────────────────────────────────────────────────────
def sap_int_to_date(col_expr):
    """Accept a Column object or string; return a nullable DateType column."""
    c = col_expr if isinstance(col_expr, F.Column) else F.col(col_expr)
    return (
        F.when(c.isNull() | (c == 0), F.lit(None).cast("date"))
         .otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))
    )

# ─────────────────────────────────────────────────────────────────────────────
# 6. Main query — join chain matching the original SQL view
# ─────────────────────────────────────────────────────────────────────────────

# 6a. Base: S032
df = s032.alias("S032")

# 6b. LEFT JOIN MARA  (WHERE MARA.INTEGRATION_INCLUDE = '1' applied later,
#     making this effectively an inner join — kept as LEFT to stay faithful)
df = df.join(
    mara.alias("MARA"),
    F.col("S032.MATNR") == F.col("MARA.MATNR"),
    "left",
)

# 6c. LEFT JOIN materialdescription (MAKT)
df = df.join(
    mat_desc.alias("MAKT"),
    F.col("MARA.MATNR") == F.col("MAKT.MATNR"),
    "left",
)

# 6d. INNER JOIN MARD  (active rows only)
df = df.join(
    mard.filter(F.col("LVORM") == "").alias("MARD"),
    (F.col("MARD.MATNR") == F.col("S032.MATNR")) &
    (F.col("MARD.WERKS") == F.col("S032.WERKS")) &
    (F.col("MARD.LGORT") == F.col("S032.LGORT")),
    "inner",
)

# 6e. LEFT JOIN fp / MBEW  (LVORM = '' already filtered inside fp CTE)
df = df.join(
    fp.alias("MBEW"),
    (F.col("MBEW.MATNR") == F.col("S032.MATNR")) &
    (F.col("MBEW.BWKEY") == F.col("S032.WERKS")) &
    (F.col("MBEW.LVORM") == ""),
    "left",
)

# 6f. LEFT JOIN procurement / MARC
df = df.join(
    procurement.alias("MARC"),
    (F.col("MARC.MATNR") == F.col("MBEW.MATNR")) &
    (F.col("MARC.WERKS") == F.col("MBEW.BWKEY")),
    "left",
)

# 6g. LEFT JOIN T001W
df = df.join(
    t001w.alias("T001W"),
    F.col("T001W.BWKEY") == F.col("MBEW.BWKEY"),
    "left",
)

# 6h. LEFT JOIN MVKE  (active rows with a non-empty lifecycle status)
df = df.join(
    mvke.filter(
        (F.col("LVORM") == "") & (F.col("VMSTA") != "")
    ).alias("MVKE"),
    (F.col("MVKE.VKORG") == F.col("T001W.VKORG")) &
    (F.col("MVKE.MATNR") == F.col("MARA.MATNR")),
    "left",
)

# 6i. WHERE  (mirrors: WHERE MARA.INTEGRATION_INCLUDE = '1')
df = df.filter(F.col("MARA.INTEGRATION_INCLUDE") == "1")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Final SELECT — match original column names exactly
#    MATNR leading-zero strip mirrors SUBSTRING + PATINDEX logic in SQL
# ─────────────────────────────────────────────────────────────────────────────
result = df.select(

    # ── Org / location ──────────────────────────────────────────────────────
    F.col("T001W.VKORG")                        .alias("Sales org."),
    F.col("S032.WERKS")                         .alias("Plant"),
    F.col("MARD.LGORT")                         .alias("Stor. Location"),
    F.col("MARD.LGPBE")                         .alias("Storage Bin"),

    # ── Material identity ────────────────────────────────────────────────────
    F.regexp_replace(F.col("MARA.MATNR"), r"^0+", "")
                                                .alias("Material"),
    F.col("MAKT.Material_Text")                 .alias("Material Text"),

    # ── Classification ──────────────────────────────────────────────────────
    F.col("MARC.BESKZ")                         .alias("Procurement"),
    F.col("MARA.MTART")                         .alias("Material Type"),
    F.col("MARA.MATKL")                         .alias("Material group"),
    F.col("MARA.SPART")                         .alias("Product group"),
    F.col("MVKE.MTPOS")                         .alias("Item cat.group"),
    F.col("MVKE.VMSTA")                         .alias("Prod Life Cycle"),

    # ── Valuation ───────────────────────────────────────────────────────────
    F.col("MBEW.BKLAS")                         .alias("Valuation Class"),
    F.col("MBEW.VPRSV")                         .alias("Price Control"),
    F.col("MBEW.PEINH").cast("decimal(15,2)")   .alias("Price Unit"),
    F.col("MBEW.VERPR").cast("decimal(15,2)")   .alias("Moving price"),
    F.col("MBEW.STPRS").cast("decimal(15,2)")   .alias("Standard price"),

    # ── Stock quantities ─────────────────────────────────────────────────────
    F.col("MARD.LABST").cast("decimal(15,2)")   .alias("Unrestricted"),
    F.col("MBEW.LBKUM").cast("decimal(15,2)")   .alias("Total Stock"),
    F.col("MBEW.SALK3").cast("decimal(15,2)")   .alias("Total Value"),

    # ── Movement dates (SAP integer YYYYMMDD → DateType) ────────────────────
    #sap_int_to_date(F.col("S032.LETZZUG"))      .alias("Last Receipt"),
    #sap_int_to_date(F.col("S032.LETZABG"))      .alias("Last gds issue"),
    #sap_int_to_date(F.col("S032.LETZVER"))      .alias("Last consumption"),
    #sap_int_to_date(F.col("S032.LETZBEW"))      .alias("Last gds mvmt"),

    sap_int_to_date(F.col("S032.LETZTZUG"))    .alias("Last Receipt"),
    sap_int_to_date(F.col("S032.LETZTABG"))    .alias("Last gds issue"),
    sap_int_to_date(F.col("S032.LETZTVER"))    .alias("Last consumption"),
    sap_int_to_date(F.col("S032.LETZTBEW"))    .alias("Last gds mvmt"),

    # ── Period info ──────────────────────────────────────────────────────────
    F.col("S032.HWAER")                         .alias("Currency"),
    F.col("MARD.LFGJA")                         .alias("Year cur.period"),
    F.col("MARD.LFMON")                         .alias("Current period"),

    # ── Integration key ──────────────────────────────────────────────────────
    F.concat(
        F.col("S032.MATNR"),
        F.col("S032.WERKS"),
        F.col("S032.LGORT"),
    )                                           .alias("INTEGRATION ID"),
)

# ─────────────────────────────────────────────────────────────────────────────
# 8. Show / persist
# ─────────────────────────────────────────────────────────────────────────────
display(result)

# Optional – write back as a Delta table:
# result.write.format("delta").mode("overwrite").saveAsTable("SCM_Bronze_LH.analytics.Current_Stock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Current Stock – PySpark (Fabric / Databricks Notebook)
# Fixed version:
#   - handles LVORM as NULL or blank
#   - removes INTEGRATION_INCLUDE='1' filter (all values are NULL in your data)
#   - uses safer ordering for latest MBEW fiscal-period row
#   - includes row-count checkpoints
# Source: SCM_Bronze_LH.sap.*
# Filters: Plant IN ('1710', 'DPSF', 'TA88')
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────────────────────────────────────
WAREHOUSE = "SCM_Bronze_LH"
SCHEMA    = "sap"
PLANTS    = ["1710", "DPSF", "TA88"]

# ─────────────────────────────────────────────────────────────────────────────
# 1. Helpers
# ─────────────────────────────────────────────────────────────────────────────
def blank_or_null(colname):
    """NULL or trimmed blank string."""
    return F.col(colname).isNull() | (F.trim(F.col(colname)) == "")

def sap_int_to_date(col_expr):
    """
    Convert SAP numeric date (YYYYMMDD stored as int/string) to DateType.
    Returns NULL when source is NULL or 0.
    """
    c = col_expr if not isinstance(col_expr, str) else F.col(col_expr)
    return (
        F.when(c.isNull() | (c == 0), F.lit(None).cast("date"))
         .otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))
    )

def rc(name, df):
    print(f"{name}: {df.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Load source tables
# ─────────────────────────────────────────────────────────────────────────────
s032  = spark.table(f"{WAREHOUSE}.{SCHEMA}.S032")
mara  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARA")
makt  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MAKT")
mard  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARD")
mbew  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MBEW_1604")
marc  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MARC")
t001w = spark.table(f"{WAREHOUSE}.{SCHEMA}.T001W")
mvke  = spark.table(f"{WAREHOUSE}.{SCHEMA}.MVKE")

# Push plant filter early
s032 = s032.filter(F.col("WERKS").isin(PLANTS))
mard = mard.filter(F.col("WERKS").isin(PLANTS))
marc = marc.filter(F.col("WERKS").isin(PLANTS))

print("=== Source counts after plant filter where applicable ===")
rc("S032", s032)
rc("MARA", mara)
rc("MAKT", makt)
rc("MARD", mard)
rc("MBEW", mbew)
rc("MARC", marc)
rc("T001W", t001w)
rc("MVKE", mvke)

# ─────────────────────────────────────────────────────────────────────────────
# 3. CTE: materialdescription
#    One material description row per MATNR
#    Exclude descriptions containing 'delete'
# ─────────────────────────────────────────────────────────────────────────────
mat_desc_raw = (
    mara.select("MATNR")
        .join(
            makt.filter(~F.lower(F.col("MAKTX")).like("%delete%"))
                .select("MATNR", "MAKTX"),
            on="MATNR",
            how="left"
        )
        .select(
            F.col("MATNR"),
            F.col("MAKTX").alias("Material_Text")
        )
        .distinct()
)

w_matdesc = Window.partitionBy("MATNR").orderBy("MATNR")

mat_desc = (
    mat_desc_raw
    .withColumn("rownum", F.row_number().over(w_matdesc))
    .filter(F.col("rownum") == 1)
    .drop("rownum")
)

rc("materialdescription", mat_desc)

# ─────────────────────────────────────────────────────────────────────────────
# 4. CTE: fp
#    Latest fiscal-period valuation row per MATNR / BWKEY / BWTAR
#    Fix: LVORM is NULL in your data, not ''
# ─────────────────────────────────────────────────────────────────────────────
w_fp = (
    Window
    .partitionBy("MATNR", "BWKEY", "BWTAR")
    .orderBy(
        F.col("LFGJA").desc(),
        F.col("LFMON").desc()
    )
)

fp = (
    mbew
    .filter(blank_or_null("LVORM"))
    .withColumn("fp_rownumber", F.row_number().over(w_fp))
    .filter(F.col("fp_rownumber") == 1)
    .select(
        "MATNR",
        "BWKEY",
        "BWTAR",
        "BKLAS",
        "VPRSV",
        "PEINH",
        "VERPR",
        "STPRS",
        "LBKUM",
        "SALK3",
        "LVORM"
    )
)

rc("fp", fp)

# ─────────────────────────────────────────────────────────────────────────────
# 5. CTE: procurement
#    MAX(BESKZ) per MATNR / WERKS from active MARC rows
# ─────────────────────────────────────────────────────────────────────────────
procurement = (
    marc
    .filter(blank_or_null("LVORM"))
    .groupBy("MATNR", "WERKS")
    .agg(F.max("BESKZ").alias("BESKZ"))
)

rc("procurement", procurement)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Main query — use aliases properly to avoid ambiguity
# ─────────────────────────────────────────────────────────────────────────────
s  = s032.alias("S032")
ma = mara.alias("MARA")
tx = mat_desc.alias("MAKT")
md = mard.filter(blank_or_null("LVORM")).alias("MARD")
mb = fp.alias("MBEW")
mc = procurement.alias("MARC")
tw = t001w.alias("T001W")
mv = mvke.filter(
    blank_or_null("LVORM") &
    (~blank_or_null("VMSTA"))
).alias("MVKE")

step1 = s
rc("step1 S032", step1)

step2 = step1.join(
    ma,
    F.col("S032.MATNR") == F.col("MARA.MATNR"),
    "left"
)
rc("step2 + MARA", step2)

step3 = step2.join(
    tx,
    F.col("MARA.MATNR") == F.col("MAKT.MATNR"),
    "left"
)
rc("step3 + MAKT", step3)

step4 = step3.join(
    md,
    (F.col("MARD.MATNR") == F.col("S032.MATNR")) &
    (F.col("MARD.WERKS") == F.col("S032.WERKS")) &
    (F.col("MARD.LGORT") == F.col("S032.LGORT")),
    "inner"
)
rc("step4 + MARD", step4)

step5 = step4.join(
    mb,
    (F.col("MBEW.MATNR") == F.col("S032.MATNR")) &
    (F.col("MBEW.BWKEY") == F.col("S032.WERKS")),
    "left"
)
rc("step5 + MBEW", step5)

step6 = step5.join(
    mc,
    (F.col("MARC.MATNR") == F.col("MBEW.MATNR")) &
    (F.col("MARC.WERKS") == F.col("MBEW.BWKEY")),
    "left"
)
rc("step6 + MARC", step6)

step7 = step6.join(
    tw,
    F.col("T001W.BWKEY") == F.col("MBEW.BWKEY"),
    "left"
)
rc("step7 + T001W", step7)

step8 = step7.join(
    mv,
    (F.col("MVKE.VKORG") == F.col("T001W.VKORG")) &
    (F.col("MVKE.MATNR") == F.col("MARA.MATNR")),
    "left"
)
rc("step8 + MVKE", step8)

# IMPORTANT:
# Removed this because your check proved it kills all rows:
# df = df.filter(F.col("MARA.INTEGRATION_INCLUDE") == "1")

df = step8

# ─────────────────────────────────────────────────────────────────────────────
# 7. Final SELECT
# ─────────────────────────────────────────────────────────────────────────────
result = df.select(

    # Org / location
    F.col("T001W.VKORG").alias("Sales org."),
    F.col("S032.WERKS").alias("Plant"),
    F.col("MARD.LGORT").alias("Stor. Location"),
    F.col("MARD.LGPBE").alias("Storage Bin"),

    # Material identity
    F.regexp_replace(F.col("MARA.MATNR"), r"^0+", "").alias("Material"),
    F.col("MAKT.Material_Text").alias("Material Text"),

    # Classification
    F.col("MARC.BESKZ").alias("Procurement"),
    F.col("MARA.MTART").alias("Material Type"),
    F.col("MARA.MATKL").alias("Material group"),
    F.col("MARA.SPART").alias("Product group"),
    F.col("MVKE.MTPOS").alias("Item cat.group"),
    F.col("MVKE.VMSTA").alias("Prod Life Cycle"),

    # Valuation
    F.col("MBEW.BKLAS").alias("Valuation Class"),
    F.col("MBEW.VPRSV").alias("Price Control"),
    F.col("MBEW.PEINH").cast("decimal(15,2)").alias("Price Unit"),
    F.col("MBEW.VERPR").cast("decimal(15,2)").alias("Moving price"),
    F.col("MBEW.STPRS").cast("decimal(15,2)").alias("Standard price"),

    # Stock quantities
    F.col("MARD.LABST").cast("decimal(15,2)").alias("Unrestricted"),
    F.col("MBEW.LBKUM").cast("decimal(15,2)").alias("Total Stock"),
    F.col("MBEW.SALK3").cast("decimal(15,2)").alias("Total Value"),

    # Movement dates
    # Using your corrected field names from Fabric
    sap_int_to_date(F.col("S032.LETZTZUG")).alias("Last Receipt"),
    sap_int_to_date(F.col("S032.LETZTABG")).alias("Last gds issue"),
    sap_int_to_date(F.col("S032.LETZTVER")).alias("Last consumption"),
    sap_int_to_date(F.col("S032.LETZTBEW")).alias("Last gds mvmt"),

    # Period info
    F.col("S032.HWAER").alias("Currency"),
    F.col("MARD.LFGJA").alias("Year cur.period"),
    F.col("MARD.LFMON").alias("Current period"),

    # Integration key
    F.concat(
        F.col("S032.MATNR"),
        F.col("S032.WERKS"),
        F.col("S032.LGORT")
    ).alias("INTEGRATION ID")
)

rc("final result", result)

display(result)

# Optional write
# result.write.format("delta").mode("overwrite").saveAsTable("SCM_Bronze_LH.analytics.Current_Stock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("S032:", s032.count())
print("MARA:", mara.count())
print("MARD:", mard.count())
print("MBEW_1604:", mbew.count())
print("MARC:", marc.count())
print("T001W:", t001w.count())
print("MVKE:", mvke.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

s032.groupBy("WERKS").count().orderBy("WERKS").show(50, False)
mard.groupBy("WERKS").count().orderBy("WERKS").show(50, False)
marc.groupBy("WERKS").count().orderBy("WERKS").show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("S032 after plant filter:", s032.filter(F.col("WERKS").isin(PLANTS)).count())
print("MARD after plant filter:", mard.filter(F.col("WERKS").isin(PLANTS)).count())
print("MARC after plant filter:", marc.filter(F.col("WERKS").isin(PLANTS)).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mard.groupBy("LVORM").count().show(20, False)
mbew.groupBy("LVORM").count().show(20, False)
marc.groupBy("LVORM").count().show(20, False)
mvke.groupBy("LVORM").count().show(20, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("MARD active:", mard.filter(F.col("LVORM") == "").count())
print("MBEW active:", mbew.filter(F.col("LVORM") == "").count())
print("MARC active:", marc.filter(F.col("LVORM") == "").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_test = s032.join(
    mard,
    (s032.MATNR == mard.MATNR) &
    (s032.WERKS == mard.WERKS) &
    (s032.LGORT == mard.LGORT),
    "inner"
)

print("S032 → MARD join:", join_test.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

join_test_active = s032.join(
    mard.filter((F.col("LVORM") == "") | F.col("LVORM").isNull()),
    (s032.MATNR == mard.MATNR) &
    (s032.WERKS == mard.WERKS) &
    (s032.LGORT == mard.LGORT),
    "inner"
)

print("S032 → MARD active:", join_test_active.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mara.groupBy("INTEGRATION_INCLUDE").count().show(20, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

s032_mara = s032.join(mara, "MATNR", "left")

print("Before filter:", s032_mara.count())

print("After filter:",
      s032_mara.filter(F.col("INTEGRATION_INCLUDE") == "1").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

s032.select("MATNR", "WERKS").distinct().show(10)
mbew.select("MATNR", "BWKEY").distinct().show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mbew_join = s032.join(
    mbew,
    (s032.MATNR == mbew.MATNR) &
    (s032.WERKS == mbew.BWKEY),
    "inner"
)

print("S032 → MBEW:", mbew_join.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mvke.select("VKORG", "MATNR").show(10)
t001w.select("BWKEY", "VKORG").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_mvke = (
    s032
    .join(mara, "MATNR", "left")
    .join(mbew, (s032.MATNR == mbew.MATNR) & (s032.WERKS == mbew.BWKEY), "left")
    .join(t001w, mbew.BWKEY == t001w.BWKEY, "left")
    .join(mvke,
          (mvke.VKORG == t001w.VKORG) &
          (mvke.MATNR == mara.MATNR),
          "inner")
)

print("MVKE join:", test_mvke.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def rc(name, df):
    print(f"{name}: {df.count()}")

step1 = s032
rc("1 S032", step1)

step2 = step1.join(mara, "MATNR", "left")
rc("2 + MARA", step2)

step3 = step2.join(mat_desc, "MATNR", "left")
rc("3 + MAKT", step3)

step4 = step3.join(
    mard,
    (step3.MATNR == mard.MATNR) &
    (step3.WERKS == mard.WERKS) &
    (step3.LGORT == mard.LGORT),
    "inner"
)
rc("4 + MARD", step4)

step5 = step4.join(
    mbew,
    (step4.MATNR == mbew_1604.MATNR) &
    (step4.WERKS == mbew_1604.BWKEY),
    "left"
)
rc("5 + MBEW", step5)

step6 = step5.join(
    marc,
    (step5.MATNR == marc.MATNR) &
    (step5.WERKS == marc.WERKS),
    "left"
)
rc("6 + MARC", step6)

step7 = step6.join(t001w, step6.WERKS == t001w.BWKEY, "left")
rc("7 + T001W", step7)

step8 = step7.filter(F.col("INTEGRATION_INCLUDE") == "1")
rc("8 final filter", step8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
