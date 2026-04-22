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

# =============================================================================
# CELL 1 – Validate source tables & columns against SCM_Bronze_LH / sap schema
# View:    analytics.Spares Consignment Orders
# Run this in a Fabric Notebook attached to SCM_Bronze_LH
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# ── 1. Dependency manifest ────────────────────────────────────────────────────
# Each entry: (table, [columns_required])
DEPENDENCIES = {
    "sap.VBAK": [
        "VBELN", "KUNNR", "AUGRU", "ERNAM", "BSTNK",
        "ERDAT", "WAERK", "AUART", "VGBEL",
    ],
    "sap.VBAP": [
        "VBELN", "POSNR", "MATNR", "WERKS", "LGORT",
        "SPART", "KWMENG", "ABGRU", "VGBEL",
    ],
    "sap.VBFA": [
        "VBELV", "POSNV", "VBELN", "POSNN", "ERDAT",
        "RFMNG", "VBTYP_N", "STUFE", "BWART",
    ],
    "sap.MSKU": [
        "MATNR", "WERKS", "KUNNR", "KULAB", "LFGJA", "LFMON",
    ],
    "sap.MAKT": [
        "MATNR", "MAKTX",
    ],
    "sap.MARA": [
        "MATNR", "PRDHA", "INTEGRATION_INCLUDE",
    ],
    "sap.KNA1": [
        "KUNNR", "NAME1", "ORT01", "REGIO", "PSTLZ",
    ],
    "sap.VBPA_SHIP_TO": [
        "VBELN", "KUNNR", "POSNR",
    ],
    "sap.VBPA_PARVW_ZR": [
        "VBELN", "PERNR",
    ],
    "sap.PA0001": [
        "PERNR", "ENAME", "ENDDA",
    ],
    "sap.MBEW": [
        "MATNR", "BWKEY", "VPRSV", "STPRS", "VERPR", "PEINH",
    ],
    "sap.SER01": [
        "LIEF_NR", "POSNR", "OBKNR",
    ],
    "sap.OBJK": [
        "OBKNR", "SERNR",
    ],
    "sap.VBEP": [
        "VBELN", "POSNR", "EDATU",
    ],
}

# ── 2. Pull actual schema from the lakehouse ──────────────────────────────────
print("Fetching available tables from SCM_Bronze_LH / sap schema …\n")

try:
    actual_tables_df = spark.sql(
        "SHOW TABLES IN SCM_Bronze_LH.sap"
    ).select(F.lower("tableName").alias("table_name"))
    actual_tables = {r.table_name for r in actual_tables_df.collect()}
except Exception as e:
    print(f"[ERROR] Could not list tables in SCM_Bronze_LH.sap: {e}")
    actual_tables = set()

# ── 3. Validate each table and its columns ────────────────────────────────────
results = []

for full_table, required_cols in DEPENDENCIES.items():
    schema_name, table_name = full_table.split(".")
    table_lower = table_name.lower()

    table_exists = table_lower in actual_tables

    if table_exists:
        try:
            actual_cols_df = spark.sql(
                f"DESCRIBE TABLE SCM_Bronze_LH.{schema_name}.{table_name}"
            )
            actual_cols = {
                r.col_name.upper()
                for r in actual_cols_df.collect()
                if not r.col_name.startswith("#")   # skip partition headers
            }
        except Exception as e:
            actual_cols = set()
            print(f"[WARN] Could not describe {full_table}: {e}")
    else:
        actual_cols = set()

    for col in required_cols:
        col_upper = col.upper()
        col_found = col_upper in actual_cols if table_exists else False
        results.append({
            "Table"         : full_table,
            "Column"        : col,
            "Table found"   : "✅" if table_exists  else "❌ MISSING",
            "Column found"  : "✅" if col_found      else ("❌ MISSING" if table_exists else "–"),
        })

# ── 4. Display results ────────────────────────────────────────────────────────
results_df = pd.DataFrame(results)

missing_tables  = results_df[results_df["Table found"]  != "✅"]["Table"].unique()
missing_cols    = results_df[
    (results_df["Table found"] == "✅") &
    (results_df["Column found"] == "❌ MISSING")
]

print("=" * 65)
print(f"  TOTAL tables required  : {len(DEPENDENCIES)}")
print(f"  Tables MISSING         : {len(missing_tables)}")
print(f"  Columns MISSING        : {len(missing_cols)}")
print("=" * 65)

# Full detail table
display(spark.createDataFrame(results_df))

# ── 5. Summary – missing tables ───────────────────────────────────────────────
if len(missing_tables):
    print("\n── MISSING TABLES ──────────────────────────────────────────")
    for t in sorted(missing_tables):
        print(f"  ❌  {t}")

# ── 6. Summary – missing columns (table exists, column absent) ────────────────
if len(missing_cols):
    print("\n── MISSING COLUMNS (table present, column absent) ──────────")
    for _, row in missing_cols.iterrows():
        print(f"  ❌  {row['Table']}.{row['Column']}")

if not len(missing_tables) and not len(missing_cols):
    print("\n  ✅  All 14 tables and 75 columns found – ready to convert to PySpark!")

# ── 7. Optional: row-count sanity check ───────────────────────────────────────
print("\n── ROW COUNTS ──────────────────────────────────────────────────")
for full_table in DEPENDENCIES:
    schema_name, table_name = full_table.split(".")
    table_lower = table_name.lower()
    if table_lower in actual_tables:
        try:
            cnt = spark.sql(
                f"SELECT COUNT(*) AS n FROM SCM_Bronze_LH.{schema_name}.{table_name}"
            ).collect()[0].n
            flag = "✅" if cnt > 0 else "⚠️  EMPTY"
            print(f"  {flag}  {full_table:<30}  {cnt:>12,} rows")
        except Exception as e:
            print(f"  ⚠️   {full_table:<30}  could not count: {e}")
    else:
        print(f"  ❌  {full_table:<30}  table not found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Fabric Notebook — Spares Consignment Orders
# Lakehouse: SCM_Bronze_LH (sap schema)
# Split each "# CELL N" block into a separate notebook cell
# ============================================================


# ── CELL 1 ── Config & helpers ─────────────────────────────
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType

spark = SparkSession.builder.getOrCreate()

LAKEHOUSE = "SCM_Bronze_LH"
SAP       = f"{LAKEHOUSE}.sap"

# ── Helper: strip SAP leading zeros (PATINDEX equivalent)
def strip_zeros(c):
    col_expr = F.col(c) if isinstance(c, str) else c
    return F.regexp_replace(col_expr, r"^0+", "")

# ── Helper: SAP integer date → Spark date (YYYYMMDD stored as int/string)
def sap_to_date(c, default="1900-01-01"):
    col_expr = F.col(c) if isinstance(c, str) else c
    return (
        F.when(col_expr.isNull() | (col_expr.cast("string").isin("0", "00000000")),
               F.lit(default).cast(DateType()))
         .otherwise(F.to_date(col_expr.cast("string"), "yyyyMMdd"))
    )

# ── Helper: unit price (STPRS if standard cost, VERPR if moving average)
def unit_price(vprsv, stprs, verpr, peinh):
    price = F.when(vprsv == "S", stprs).otherwise(verpr)
    denom = F.when(peinh == F.lit(0), F.lit(None)).otherwise(peinh)
    return price / denom

# ── Helper: consignment qty inner logic (reused in both UNION branches)
def cons_qty_expr(kulab, net_issued):
    return F.coalesce(
        F.when(kulab > 0,
            F.when(kulab < net_issued, kulab)
             .otherwise(net_issued)
        ).otherwise(kulab).cast(DecimalType(15, 2)),
        F.lit(0).cast(DecimalType(15, 2))
    )

print("✅  Cell 1 – config ready")


# ── CELL 2 ── Load raw source tables ───────────────────────
vbak_raw    = spark.table(f"{SAP}.VBAK")
vbap_raw    = spark.table(f"{SAP}.VBAP")
vbfa_raw    = spark.table(f"{SAP}.VBFA")
msku_raw    = spark.table(f"{SAP}.MSKU")
makt_raw    = spark.table(f"{SAP}.MAKT")
mara_raw    = spark.table(f"{SAP}.MARA")
kna1_raw    = spark.table(f"{SAP}.KNA1")
vbpa_ship   = spark.table(f"{SAP}.VBPA_SHIP_TO")
pa0001_raw  = spark.table(f"{SAP}.PA0001")
mbew_raw    = spark.table(f"{SAP}.MBEW")
ser01_raw   = spark.table(f"{SAP}.SER01")
objk_raw    = spark.table(f"{SAP}.OBJK")
vbep_raw    = spark.table(f"{SAP}.VBEP")
vbpa_raw    = spark.table(f"{SAP}.VBPA_PARVW_ZR")

# ── VBPA_PARVW_ZR: try full VBPA table filtered on PARVW='ZR',
#    fallback to empty DataFrame with required schema
try:
    vbpa_zr_raw = (
        spark.table(f"{SAP}.VBPA")
             .filter(F.col("PARVW") == "ZR")
             .select("VBELN", "PERNR")
    )
    print("✅  sap.VBPA found – filtering PARVW='ZR'")
except Exception:
    from pyspark.sql.types import StructType, StructField
    vbpa_zr_raw = spark.createDataFrame(
        [],
        StructType([
            StructField("VBELN", StringType()),
            StructField("PERNR", StringType()),
        ])
    )
    print("⚠️   sap.VBPA not found – Personnel No. / Empl. Name will be NULL")

print("✅  Cell 2 – raw tables loaded")


# ── CELL 3 ── CTE: VBAK (KB order headers) ─────────────────
vbak = (
    vbak_raw
    .filter(F.col("AUART").isin("ZDRE"))
    .select("VBELN", "KUNNR", "AUGRU", "ERNAM", "BSTNK", "ERDAT", "WAERK")
)

print(f"✅  VBAK  → {vbak.count():,} KB order headers")


# ── CELL 4 ── CTE: MSKU_DATA (latest consignment stock per material/plant/customer)
_w_msku = Window.partitionBy("MATNR", "WERKS", "KUNNR").orderBy(
    F.concat(F.col("LFGJA"), F.col("LFMON")).desc()
)
msku_data = (
    msku_raw
    .withColumn("rn", F.row_number().over(_w_msku))
    .filter(F.col("rn") == 1)
    .select("MATNR", "WERKS", "KUNNR", "KULAB", "LFGJA")
)

print(f"✅  MSKU_DATA  → {msku_data.count():,} rows")

# ── CELL 5 ── CTE: MAKT (first non-deleted material description)
_w_makt = Window.partitionBy("MATNR").orderBy("MATNR")
makt = (
    makt_raw
    .filter(~F.lower(F.col("MAKTX")).contains("delete"))
    .withColumn("rn", F.row_number().over(_w_makt))
    .filter(F.col("rn") == 1)
    .select("MATNR", F.col("MAKTX").alias("Material Text"))
)

print(f"✅  MAKT  → {makt.count():,} rows")


# ── CELL 6 ── CTE: VBFA_Return_Order (latest return doc per source line)
vbfa_return_order = (
    vbfa_raw
    .filter(F.col("VBTYP_N") == "T")
    .groupBy("VBELV", "POSNV")
    .agg(F.max("ERDAT").alias("ERDAT"))
)


print(f"✅  VBFA_Return_Order  → {vbfa_return_order.count():,} rows")


# ── CELL 7 ── CTE: VBFA_IssueQty_631 (goods issue movement 631)
_w_iq631 = (
    Window.partitionBy("VBELV", "POSNV")
          .orderBy(F.col("VBELV"), F.col("POSNV"), F.col("ERDAT").desc())
)
vbfa_iq631 = (
    vbfa_raw
    .filter(
        (F.col("VBTYP_N").isin("R", "h")) &
        (F.col("STUFE") == "01") &
        (F.col("BWART") == "631")
    )
    .withColumn("rn", F.row_number().over(_w_iq631))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "ERDAT", F.col("RFMNG").alias("Issue qty"))
)

print(f"✅  VBFA_IssueQty_631  → {vbfa_iq631.count():,} rows")


# ── CELL 8 ── CTE: VBFA_IssueQty_NOT631 (non-631 movements, negated)
_w_iqn631 = (
    Window.partitionBy("VBELV", "POSNV")
          .orderBy(F.col("VBELV"), F.col("POSNV"), F.col("ERDAT").desc())
)
vbfa_iqn631 = (
    vbfa_raw
    .filter(
        (F.col("VBTYP_N").isin("R", "h")) &
        (F.col("STUFE") == "01") &
        (F.col("BWART") != "631")
    )
    .withColumn("rn", F.row_number().over(_w_iqn631))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "ERDAT", (-F.col("RFMNG")).alias("Issue qty"))
)

print(f"✅  VBFA_IssueQty_NOT631  → {vbfa_iqn631.count():,} rows")


# ── CELL 9 ── CTE: VBFA_IssueQty (FULL OUTER JOIN – 631 preferred)
_631  = vbfa_iq631.select("VBELV", "POSNV",
                           F.col("Issue qty").alias("iq_631"))
_n631 = vbfa_iqn631.select("VBELV", "POSNV",
                             F.col("Issue qty").alias("iq_not631"))

vbfa_issue_qty = (
    _631.join(_n631, ["VBELV", "POSNV"], how="full")
        .select(
            F.col("VBELV"),
            F.col("POSNV"),
            F.coalesce(F.col("iq_631"), F.col("iq_not631")).alias("Issue qty"),
        )
)

print(f"✅  VBFA_IssueQty  → {vbfa_issue_qty.count():,} rows")


# ── CELL 10 ── CTE: VBFA_IssueDt (issue date from 631)
_w_idt = Window.partitionBy("VBELV", "POSNV").orderBy(F.col("ERDAT").desc())
vbfa_issue_dt = (
    vbfa_raw
    .filter(
        (F.col("VBTYP_N").isin("R", "h")) &
        (F.col("STUFE") == "01") &
        (F.col("BWART") == "631")
    )
    .withColumn("rn", F.row_number().over(_w_idt))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", F.col("ERDAT").alias("Issue date"))
)

print(f"✅  VBFA_IssueDt  → {vbfa_issue_dt.count():,} rows")


# ── CELL 11 ── CTE: VBFA_ReturnQty (STUFE=02; 632=positive, others negated)
_w_rq = Window.partitionBy("VBELV", "POSNV").orderBy(F.col("ERDAT").desc())
vbfa_return_qty = (
    vbfa_raw
    .filter(
        (F.col("VBTYP_N").isin("R", "h")) &
        (F.col("STUFE") == "02")
    )
    .withColumn(
        "Return qty",
        F.when(F.col("BWART") == "632",  F.col("RFMNG"))
         .otherwise(-F.col("RFMNG"))
    )
    .withColumn("rn", F.row_number().over(_w_rq))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "Return qty")
)

print(f"✅  VBFA_ReturnQty  → {vbfa_return_qty.count():,} rows")


# ── CELL 12 ── CTE: VBFA_ReturnDt (return date from 632)
_w_rdt = Window.partitionBy("VBELV", "POSNV").orderBy(F.col("ERDAT").desc())
vbfa_return_dt = (
    vbfa_raw
    .filter(
        (F.col("VBTYP_N").isin("R", "h")) &
        (F.col("STUFE") == "02") &
        (F.col("BWART") == "632")
    )
    .withColumn("rn", F.row_number().over(_w_rdt))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", F.col("ERDAT").alias("Return date"))
)

print(f"✅  VBFA_ReturnDt  → {vbfa_return_dt.count():,} rows")


# ── CELL 13 ── CTE: VBAP (KB self-joined to KA via VGBEL)
_kb = vbap_raw.filter(F.col("MATNR") != "").alias("KB")
_ka = vbap_raw.alias("KA")

vbap = (
    _kb.join(
        _ka,
        (F.col("KB.VBELN") == F.col("KA.VGBEL")) &
        (F.col("KB.POSNR") == F.col("KA.POSNR")),
        how="left"
    )
    .select(
        F.col("KB.VBELN").alias("KB_VBELN"),
        F.col("KA.VBELN").alias("KA_VBELN"),
        F.col("KB.LGORT").alias("KB_LGORT"),
        F.col("KA.LGORT").alias("KA_LGORT"),
        F.col("KB.POSNR").alias("KB_POSNR"),
        F.col("KA.POSNR").alias("KA_POSNR"),
        F.col("KB.WERKS").alias("KB_WERKS"),
        F.col("KA.WERKS").alias("KA_WERKS"),
        F.col("KB.SPART").alias("KB_SPART"),
        F.col("KA.SPART").alias("KA_SPART"),
        F.col("KB.MATNR").alias("KB_MATNR"),
        F.col("KA.MATNR").alias("KA_MATNR"),
        F.col("KB.KWMENG").alias("KB_KWMENG"),
        F.col("KA.KWMENG").alias("KA_KWMENG"),
        F.col("KB.ABGRU").alias("KB_ABGRU"),
        F.col("KA.ABGRU").alias("KA_ABGRU"),
    ) 
)



print(f"✅  VBAP  → {vbap.count():,} rows")


# ── CELL 14 ── CTE: VBFA_SERIAL (delivery doc links for serial numbers)
vbfa_serial = (
    vbfa_raw
    .filter(F.col("VBTYP_N") == "K")
    .select("VBELV", "POSNV", "VBELN", "POSNN")
)

print(f"✅  VBFA_SERIAL  → {vbfa_serial.count():,} rows")


# ── CELL 15 ── CTE: SerialNO (STRING_AGG equivalent via collect_list)
serial_no = (
    ser01_raw
    .join(objk_raw, "OBKNR")
    .withColumn("SERNR_clean", strip_zeros("SERNR"))
    .groupBy("LIEF_NR", "POSNR")
    .agg(
        F.array_join(
            F.array_sort(F.collect_list(F.col("SERNR_clean"))),
            ","
        ).alias("SERNR")
    )
)

print(f"✅  SerialNO  → {serial_no.count():,} serial number groups")


# ── CELL 16 ── CTE: MARA (product hierarchy; INTEGRATION_INCLUDE removed – column absent)
mara = mara_raw.select("MATNR", "PRDHA")

print(f"✅  MARA  → {mara.count():,} rows  (no INTEGRATION_INCLUDE filter – column not in lakehouse)")


# ── CELL 17 ── CTE: PA0001 (active employee records)
pa0001 = (
    pa0001_raw
    .filter(F.col("ENDDA") == "99991231")
    .select("PERNR", "ENAME")
)

print(f"✅  PA0001  → {pa0001.count():,} active employees")


# ── CELL 18 ── CTE: VBEP (schedule line dates)
vbep = vbep_raw.select("VBELN", "POSNR", "EDATU")

print(f"✅  VBEP  → {vbep.count():,} schedule lines")


# ── CELL 19 ── CTE: VBPA_PARVW_ZR (sales rep per document)
vbpa_zr = vbpa_zr_raw.select("VBELN", "PERNR")

print(f"✅  VBPA_ZR  → {vbpa_zr.count():,} rows")


# ── CELL 20 ── FIRST SELECT: KB orders that have a matching KA/ZR order ──────
#
# Alias guide:
#   vbak        → KB order header
#   ka_order    → KA/ZR order header (self-join on VGBEL)
#   kna1        → sold-to customer master
#   kna_ship    → ship-to customer master
#   vbpa_ship   → ship-to partner function

ka_order = (
    vbak_raw
    .filter(F.col("AUART").isin("ZKA", "ZR"))
    .select(
        F.col("VBELN").alias("KA_VBELN"),
        F.col("KUNNR").alias("KA_KUNNR"),
        F.col("VGBEL").alias("KA_VGBEL"),
        F.col("ERNAM").alias("KA_ERNAM"),
        F.col("ERDAT").alias("KA_ERDAT"),
    )
)

kna1     = kna1_raw.alias("KNA1")
kna_ship = kna1_raw.alias("KNA_SHIP")

# ── Build first SELECT base via sequential LEFT JOINs
df1 = (
    vbak
    .alias("VBAK")

    # KA/ZR order header
    # FIX: added .alias("ka_order") so Spark resolves ka_order.* column references
    .join(ka_order.alias("ka_order"),
          (F.col("ka_order.KA_KUNNR") == F.col("VBAK.KUNNR")) &
          (F.col("ka_order.KA_VGBEL") == F.col("VBAK.VBELN")),
          how="left")

    # Sold-to customer master
    .join(kna1,   F.col("VBAK.KUNNR") == F.col("KNA1.KUNNR"),   how="left")

    # Item data (KB↔KA)
    .join(vbap.alias("VBAP"), F.col("VBAP.KB_VBELN") == F.col("VBAK.VBELN"), how="left")

    # Ship-to partner
    .join(vbpa_ship.alias("VBPA_SHIP"),
          F.col("VBAK.VBELN") == F.col("VBPA_SHIP.VBELN"),
          how="left")

    # Ship-to customer master
    .join(kna_ship,
          F.col("KNA_SHIP.KUNNR") == F.col("VBPA_SHIP.KUNNR"),
          how="left")

    # Consignment stock
    .join(msku_data.alias("MSKU"),
          (F.col("VBAP.KA_MATNR") == F.col("MSKU.MATNR")) &
          (F.col("VBAK.KUNNR")    == F.col("MSKU.KUNNR"))  &
          (F.col("VBAP.KA_WERKS") == F.col("MSKU.WERKS")),
          how="left")

    # Material hierarchy
    .join(mara.alias("MARA"),  F.col("MARA.MATNR")  == F.col("VBAP.KA_MATNR"), how="left")

    # Material description
    .join(makt.alias("MAKT"),  F.col("MAKT.MATNR")  == F.col("VBAP.KA_MATNR"), how="left")

    # Valuation (plant-level)
    .join(mbew_raw.alias("MBEW"),
          (F.col("MBEW.MATNR") == F.col("VBAP.KA_MATNR")) &
          (F.col("MBEW.BWKEY") == F.col("VBAP.KA_WERKS")),
          how="left")

    # Sales rep
    .join(vbpa_zr.alias("VBPA_ZR"),
          F.col("VBPA_ZR.VBELN") == F.col("VBAK.VBELN"),
          how="left")

    # Employee name
    .join(pa0001.alias("PA0001"),
          F.col("PA0001.PERNR") == F.col("VBPA_ZR.PERNR"),
          how="left")

    # Issue qty/date
    .join(vbfa_issue_qty.alias("IQ"),
          (F.col("IQ.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("IQ.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")
    .join(vbfa_issue_dt.alias("IDT"),
          (F.col("IDT.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("IDT.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")

    # Return qty/date (keyed on KA_POSNR)
    .join(vbfa_return_qty.alias("RQ"),
          (F.col("RQ.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("RQ.POSNV") == F.col("VBAP.KA_POSNR")),
          how="left")
    .join(vbfa_return_dt.alias("RDT"),
          (F.col("RDT.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("RDT.POSNV") == F.col("VBAP.KA_POSNR")),
          how="left")

    # Serial numbers
    .join(vbfa_serial.alias("VSER"),
          (F.col("VSER.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("VSER.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")
    .join(serial_no.alias("SERNO"),
          (F.col("SERNO.LIEF_NR") == F.col("VSER.VBELN")) &
          (F.col("SERNO.POSNR")   == F.col("VSER.POSNN")),
          how="left")

    # Schedule line
    .join(vbep.alias("VBEP"),
          (F.col("VBEP.VBELN") == F.col("VBAK.VBELN")) &
          (F.col("VBEP.POSNR") == F.col("VBAP.KB_POSNR")),
          how="left")

    # Only non-rejected items that have a KA match
    .filter(
        (F.col("VBAP.KA_ABGRU") == "") &
        (F.col("VBAP.KB_ABGRU") == "")
    )
) 

# ── Computed expressions ──────────────────────────────────
_net1     = F.coalesce(F.col("IQ.`Issue qty`"),  F.lit(0)) - \
            F.coalesce(F.col("RQ.`Return qty`"), F.lit(0))
_uprice1  = unit_price(
                F.col("MBEW.VPRSV"),
                F.col("MBEW.STPRS"),
                F.col("MBEW.VERPR"),
                F.col("MBEW.PEINH")
            )
_end_dt1  = F.coalesce(
                F.when(
                    F.col("RDT.`Return date`").isNull() |
                    (F.coalesce(F.col("RQ.`Return qty`"), F.lit(0)) != F.col("VBAP.KB_KWMENG")),
                    sap_to_date(F.col("VBEP.EDATU"), default=None)
                ).otherwise(sap_to_date(F.col("RDT.`Return date`"), default=None)),
                sap_to_date(F.col("VBEP.EDATU"), default=None)
            )

select1 = df1.select(
    strip_zeros(F.col("VBAK.VBELN"))          .alias("KB order"),
    strip_zeros(F.col("ka_order.KA_VBELN"))    .alias("KA/ZR order"),
    strip_zeros(F.col("VBAK.KUNNR"))           .alias("Customer"),
    F.col("VBAK.AUGRU")                        .alias("Order reason"),
    F.col("KNA1.NAME1")                        .alias("Name 1"),
    F.col("KNA1.ORT01")                        .alias("City"),
    F.col("KNA1.REGIO")                        .alias("Region"),
    F.col("KNA1.PSTLZ")                        .alias("Postal Code"),
    F.col("KNA_SHIP.NAME1")                    .alias("Ship-To Name"),
    F.col("VBAP.KA_WERKS")                     .alias("Plant"),
    F.col("VBAP.KB_LGORT")                     .alias("Stor. Location"),
    F.col("VBAP.KA_SPART")                     .alias("Product group"),
    strip_zeros(F.col("VBAP.KA_MATNR"))        .alias("Material"),
    F.col("MAKT.`Material Text`")              .alias("Description"),
    strip_zeros(F.col("ka_order.KA_VBELN"))    .alias("Sales Document"),
    strip_zeros(F.col("KNA_SHIP.KUNNR"))       .alias("Ship-To Number"),
    strip_zeros(F.col("VBAP.KA_POSNR"))        .alias("Item"),
    F.col("MARA.PRDHA")                        .alias("prod_hierarchy"),
    F.col("VBPA_ZR.PERNR")                     .alias("Personnel No."),
    F.col("PA0001.ENAME")                      .alias("Empl./Appl. Name"),
    F.col("VBAK.ERNAM")                        .alias("Created by (KB)"),
    F.col("ka_order.KA_ERNAM")                 .alias("Created by (KA)"),
    F.col("VBAK.BSTNK")                        .alias("Customer Reference"),
    sap_to_date(F.col("VBAK.ERDAT"))           .alias("KB Creation date"),
    sap_to_date(F.col("ka_order.KA_ERDAT"))    .alias("KA Creation date"),
    F.concat_ws("",
        F.col("VBAK.VBELN"),
        F.col("VBAP.KA_POSNR"),
        F.col("ka_order.KA_VBELN"))            .alias("INTEGRATION_ID"),
    F.col("SERNO.SERNR")                       .alias("Serial Number"),
    # Cons qty
    

    cons_qty_expr(F.col("MSKU.KULAB"), _net1)  .alias("Cons qty"),
    # Cons val
    F.coalesce(
        (_uprice1 * cons_qty_expr(F.col("MSKU.KULAB"), _net1)).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                           .alias("Cons val"),
    # Con Days
    F.when(
        F.col("IDT.`Issue date`").isNotNull() & _end_dt1.isNotNull(),
        F.datediff(_end_dt1, sap_to_date(F.col("IDT.`Issue date`"), default=None))
    ).otherwise(F.lit(None))                    .alias("Con Days"),
    # Issue status
    F.when(F.col("VBAP.KB_KWMENG") == F.coalesce(F.col("IQ.`Issue qty`"), F.lit(0)),
           F.lit("CmpGI")).otherwise(F.lit("IncGI"))
                                               .alias("Issue status"),
    # Issue qty
    F.coalesce(F.col("IQ.`Issue qty`").cast(DecimalType(15,2)),
               F.lit(0).cast(DecimalType(15,2)))
                                               .alias("Issue qty"),
    # Issue val
    F.coalesce(
        (_uprice1 * F.coalesce(F.col("IQ.`Issue qty`"), F.lit(0))).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                          .alias("Issue val"),
    # Issue date
    sap_to_date(F.col("IDT.`Issue date`"), default=None)
                                               .alias("Issue date"),
    # Order qty
    F.col("VBAP.KB_KWMENG").cast(DecimalType(15,2))
                                               .alias("Order qty"),
    # Order val
    (_uprice1 * F.col("VBAP.KB_KWMENG")).cast(DecimalType(18,2))
                                               .alias("Order val"),
    # Return status
    F.when(F.col("VBAP.KB_KWMENG") == F.coalesce(F.col("RQ.`Return qty`"), F.lit(0)),
           F.lit("CmpGI")).otherwise(F.lit("IncGI"))
                                               .alias("Return status"),
    # Return qty
    F.coalesce(F.col("RQ.`Return qty`").cast(DecimalType(15,2)),
               F.lit(0).cast(DecimalType(15,2)))
                                               .alias("Return qty"),
    # Return val
    F.coalesce(
        (_uprice1 * F.coalesce(F.col("RQ.`Return qty`"), F.lit(0))).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                          .alias("Return val"),
    # Return date
    F.when(
        F.col("RDT.`Return date`").isNull() |
        (F.coalesce(F.col("RQ.`Return qty`"), F.lit(0)) != F.col("VBAP.KB_KWMENG")),
        sap_to_date(F.col("VBEP.EDATU"), default=None)
    ).otherwise(
        sap_to_date(F.col("RDT.`Return date`"), default=None)
    )                                          .alias("Return date"),
    F.col("VBAK.WAERK")                        .alias("Currency"),
) 


print(f"✅  First SELECT  → {select1.count():,} rows")



# ── CELL 21 ── SECOND SELECT: KB orders with NO matching KA/ZR order ──────────
df2 = (
    vbak
    .alias("VBAK")

    # Sold-to customer master
    .join(kna1, F.col("VBAK.KUNNR") == F.col("KNA1.KUNNR"), how="left")

    # Item data
    .join(vbap.alias("VBAP"), F.col("VBAP.KB_VBELN") == F.col("VBAK.VBELN"), how="left")

    # Ship-to partner
    .join(vbpa_ship.alias("VBPA_SHIP"),
          (F.col("VBAP.KB_VBELN")       == F.col("VBPA_SHIP.VBELN")) &
          (F.col("VBPA_SHIP.POSNR")     == F.lit("000000")),
          how="left")

    # Ship-to customer master
    .join(kna_ship,
          F.col("KNA_SHIP.KUNNR") == F.col("VBPA_SHIP.KUNNR"),
          how="left")

    # Consignment stock
    .join(msku_data.alias("MSKU"),
          (F.col("VBAP.KB_MATNR") == F.col("MSKU.MATNR")) &
          (F.col("VBAK.KUNNR")    == F.col("MSKU.KUNNR"))  &
          (F.col("VBAP.KB_WERKS") == F.col("MSKU.WERKS")),
          how="left")

    # Material hierarchy + description
    .join(mara.alias("MARA"),  F.col("MARA.MATNR") == F.col("VBAP.KB_MATNR"), how="left")
    .join(makt.alias("MAKT"),  F.col("MAKT.MATNR") == F.col("MARA.MATNR"),    how="left")

    # Valuation (plant-level)
    .join(mbew_raw.alias("MBEW"),
          (F.col("MBEW.MATNR") == F.col("VBAP.KB_MATNR")) &
          (F.col("MBEW.BWKEY") == F.col("VBAP.KB_WERKS")),
          how="left")

    # Sales rep
    .join(vbpa_zr.alias("VBPA_ZR"),
          F.col("VBPA_ZR.VBELN") == F.col("VBAP.KB_VBELN"),
          how="left")
    .join(pa0001.alias("PA0001"),
          F.col("PA0001.PERNR") == F.col("VBPA_ZR.PERNR"),
          how="left")

    # Issue qty/date
    .join(vbfa_issue_qty.alias("IQ"),
          (F.col("IQ.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("IQ.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")
    .join(vbfa_issue_dt.alias("IDT"),
          (F.col("IDT.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("IDT.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")

    # Return qty/date (keyed on KB_POSNR for second SELECT)
    .join(vbfa_return_qty.alias("RQ"),
          (F.col("RQ.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("RQ.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")
    .join(vbfa_return_dt.alias("RDT"),
          (F.col("RDT.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("RDT.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")

    # Serial numbers
    .join(vbfa_serial.alias("VSER"),
          (F.col("VSER.VBELV") == F.col("VBAP.KB_VBELN")) &
          (F.col("VSER.POSNV") == F.col("VBAP.KB_POSNR")),
          how="left")
    .join(serial_no.alias("SERNO"),
          (F.col("SERNO.LIEF_NR") == F.col("VSER.VBELN")) &
          (F.col("SERNO.POSNR")   == F.col("VSER.POSNN")),
          how="left")

    # Schedule line
    .join(vbep.alias("VBEP"),
          (F.col("VBEP.VBELN") == F.col("VBAK.VBELN")) &
          (F.col("VBEP.POSNR") == F.col("VBAP.KB_POSNR")),
          how="left")

    # Only non-rejected KB items with no KA order
    .filter(
        (F.col("VBAP.KB_ABGRU") == "") &
        F.col("VBAP.KA_VBELN").isNull()   # no KA match
    )
)

# ── Computed expressions ──────────────────────────────────
_net2    = F.coalesce(F.col("IQ.`Issue qty`"),  F.lit(0)) - \
           F.coalesce(F.col("RQ.`Return qty`"), F.lit(0))
_uprice2 = unit_price(
               F.col("MBEW.VPRSV"),
               F.col("MBEW.STPRS"),
               F.col("MBEW.VERPR"),
               F.col("MBEW.PEINH")
           )
_end_dt2 = F.when(
               F.col("RDT.`Return date`").isNull() |
               (F.coalesce(F.col("RQ.`Return qty`"), F.lit(0)) != F.col("VBAP.KB_KWMENG")),
               sap_to_date(F.col("VBEP.EDATU"), default=None)
           ).otherwise(sap_to_date(F.col("RDT.`Return date`"), default=None))

select2 = df2.select(
    strip_zeros(F.col("VBAK.VBELN"))         .alias("KB order"),
    F.lit(None).cast(StringType())            .alias("KA/ZR order"),
    strip_zeros(F.col("VBAK.KUNNR"))          .alias("Customer"),
    F.col("VBAK.AUGRU")                       .alias("Order reason"),
    F.col("KNA1.NAME1")                       .alias("Name 1"),
    F.col("KNA1.ORT01")                       .alias("City"),
    F.col("KNA1.REGIO")                       .alias("Region"),
    F.col("KNA1.PSTLZ")                       .alias("Postal Code"),
    F.col("KNA_SHIP.NAME1")                   .alias("Ship-To Name"),
    F.col("VBAP.KB_WERKS")                    .alias("Plant"),
    F.col("VBAP.KB_LGORT")                    .alias("Stor. Location"),
    F.col("VBAP.KB_SPART")                    .alias("Product group"),
    strip_zeros(F.col("VBAP.KB_MATNR"))       .alias("Material"),
    F.col("MAKT.`Material Text`")             .alias("Description"),
    F.lit(None).cast(StringType())            .alias("Sales Document"),
    strip_zeros(F.col("KNA1.KUNNR"))          .alias("Ship-To Number"),
    strip_zeros(F.col("VBAP.KB_POSNR"))       .alias("Item"),
    F.col("MARA.PRDHA")                       .alias("prod_hierarchy"),
    F.col("VBPA_ZR.PERNR")                    .alias("Personnel No."),
    F.col("PA0001.ENAME")                     .alias("Empl./Appl. Name"),
    F.col("VBAK.ERNAM")                       .alias("Created by (KB)"),
    F.lit(None).cast(StringType())            .alias("Created by (KA)"),
    F.col("VBAK.BSTNK")                       .alias("Customer Reference"),
    sap_to_date(F.col("VBAK.ERDAT"))          .alias("KB Creation date"),
    F.lit(None).cast(DateType())              .alias("KA Creation date"),
    F.concat_ws("",
        F.col("VBAK.VBELN"),
        F.col("VBAP.KB_POSNR"),
        F.col("VBAK.VBELN"))                  .alias("INTEGRATION_ID"),
    F.col("SERNO.SERNR")                      .alias("Serial Number"),
    # Cons qty
    cons_qty_expr(F.col("MSKU.KULAB"), _net2) .alias("Cons qty"),
    # Cons val
    F.coalesce(
        (_uprice2 * cons_qty_expr(F.col("MSKU.KULAB"), _net2)).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                          .alias("Cons val"),
    # Con Days
    F.when(
        F.col("IDT.`Issue date`").isNotNull() & _end_dt2.isNotNull(),
        F.datediff(_end_dt2, sap_to_date(F.col("IDT.`Issue date`"), default=None))
    ).otherwise(F.lit(None))                   .alias("Con Days"),
    # Issue status
    F.when(F.col("VBAP.KB_KWMENG") == F.coalesce(F.col("IQ.`Issue qty`"), F.lit(0)),
           F.lit("CmpGI")).otherwise(F.lit("IncGI"))
                                               .alias("Issue status"),
    # Issue qty
    F.coalesce(F.col("IQ.`Issue qty`").cast(DecimalType(15,2)),
               F.lit(0).cast(DecimalType(15,2)))
                                               .alias("Issue qty"),
    # Issue val
    F.coalesce(
        (_uprice2 * F.coalesce(F.col("IQ.`Issue qty`"), F.lit(0))).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                          .alias("Issue val"),
    # Issue date
    sap_to_date(F.col("IDT.`Issue date`"), default=None)
                                               .alias("Issue date"),
    # Order qty
    F.col("VBAP.KB_KWMENG").cast(DecimalType(15,2))
                                               .alias("Order qty"),
    # Order val
    (_uprice2 * F.col("VBAP.KB_KWMENG")).cast(DecimalType(18,2))
                                               .alias("Order val"),
    F.lit(None).cast(StringType())            .alias("Return status"),
    # Return qty
    F.coalesce(F.col("RQ.`Return qty`").cast(DecimalType(15,2)),
               F.lit(0).cast(DecimalType(15,2)))
                                               .alias("Return qty"),
    # Return val
    F.coalesce(
        (_uprice2 * F.coalesce(F.col("RQ.`Return qty`"), F.lit(0))).cast(DecimalType(18,2)),
        F.lit(0).cast(DecimalType(18,2))
    )                                          .alias("Return val"),
    F.lit(None).cast(DateType())              .alias("Return date"),
    F.col("VBAK.WAERK")                       .alias("Currency"),
)

print(f"✅  Second SELECT  → {select2.count():,} rows")


# ── CELL 22 ── UNION ALL & output row count ────────────────
result = select1.unionAll(select2)

total = result.count()
print(f"✅  Final result  → {total:,} total rows")
print(f"    SELECT 1  : {select1.count():,}")
print(f"    SELECT 2  : {select2.count():,}")
display(result)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# # Start

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, max as _max, row_number, concat, desc, lit, when, 
    regexp_replace, concat_ws, collect_list, to_date, 
    coalesce, expr, datediff
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VBAK_raw = (
    spark.table("SCM_Bronze_LH.sap.VBAK")
    .filter(F.col("MANDT") == "100")
    .filter(F.col("AUART").isin("KB", "ZKBB", "ZKDD", "ZMOR", "ZSOR"))
)
MSKU_raw   = spark.table("SCM_Bronze_LH.sap.MSKU").filter("MANDT = '100'")
MAKT_raw   = spark.table("SCM_Bronze_LH.sap.MAKT").filter("MANDT = '100'")
VBFA_raw   = spark.table("SCM_Bronze_LH.sap.VBFA").filter("MANDT = '100'")
VBEP_raw   = spark.table("SCM_Bronze_LH.sap.VBEP").filter("MANDT = '100'")
VBAP_raw   = spark.table("SCM_Bronze_LH.sap.VBAP").filter("MANDT = '100'")
SER01_raw  = spark.table("SCM_Bronze_LH.sap.SER01").filter("MANDT = '100'")
OBJK_raw   = spark.table("SCM_Bronze_LH.sap.OBJK").filter("MANDT = '100'")
MARA_raw   = spark.table("SCM_Bronze_LH.sap.MARA").filter("MANDT = '100'")
PA0001_raw = spark.table("SCM_Bronze_LH.sap.PA0001").filter("MANDT = '100'")
KNA1_raw   = spark.table("SCM_Bronze_LH.sap.KNA1").filter("MANDT = '100'")
VBPA_SHIP  = spark.table("SCM_Bronze_LH.sap.VBPA_SHIP_TO")
VBPA_ZR    = spark.table("SCM_Bronze_LH.sap.VBPA_PARVW_ZR").filter("MANDT = '100'")
MBEW_raw   = spark.table("SCM_Bronze_LH.sap.MBEW").filter("MANDT = '100'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VBAK = (
    VBAK_raw
    .filter(F.col("AUART").isin("KB", "ZKBB", "ZKDD","ZMOR","ZSOR"))
    .select("VBELN", "KUNNR", "AUGRU", "ERNAM", "BSTNK", "ERDAT", "WAERK")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(VBAK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

msku_window = Window.partitionBy("MATNR", "WERKS", "KUNNR").orderBy(
    F.concat(F.col("LFGJA"), F.col("LFMON")).desc()
)

MSKU_DATA = (
    MSKU_raw
    .withColumn("rn", F.row_number().over(msku_window))
    .filter(F.col("rn") == 1)
    .select("MATNR", "WERKS", "KUNNR", "KULAB", "LFGJA")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

makt_window = Window.partitionBy("MATNR").orderBy("MATNR")

MAKT = (
    MAKT_raw
    .filter(~F.col("MAKTX").like("%delete%"))
    .withColumn("rn", F.row_number().over(makt_window))
    .filter(F.col("rn") == 1)
    .select(F.col("MATNR"), F.col("MAKTX").alias("Material Text"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VBFA_Return_Order = (
    VBFA_raw
    .filter(F.col("VBTYP_N") == "T")
    .groupBy("VBELV", "POSNV")
    .agg(F.max("ERDAT").alias("ERDAT"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_631_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc()
)

VBFA_IssueQty_631 = (
    VBFA_raw
    .filter(
        F.col("VBTYP_N").isin("B", "b") 
        #&
        # (F.col("STUFE") == "01") &
        # (F.col("BWART") == "631")
    )
    .withColumn("Issue qty", F.col("RFMNG"))
    .withColumn("rn", F.row_number().over(vbfa_631_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "ERDAT", "Issue qty")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_not631_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc()
)

VBFA_IssueQty_NOT631 = (
    VBFA_raw
    .filter(
        F.col("VBTYP_N").isin("R", "h") &
        (F.col("STUFE") == "01") &
        (F.col("BWART") != "631")
    )
    .withColumn("Issue qty", -F.col("RFMNG"))
    .withColumn("rn", F.row_number().over(vbfa_not631_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "ERDAT", "Issue qty")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VBFA_IssueQty = (
    VBFA_IssueQty_631.alias("a")
    .join(
        VBFA_IssueQty_NOT631.alias("b"),
        (F.col("a.VBELV") == F.col("b.VBELV")) &
        (F.col("a.POSNV") == F.col("b.POSNV")),
        how="fullouter"
    )
    .select(
        F.when(F.col("a.`Issue qty`").isNotNull(), F.col("a.VBELV"))
         .otherwise(F.col("b.VBELV")).alias("VBELV"),
        F.when(F.col("a.`Issue qty`").isNotNull(), F.col("a.POSNV"))
         .otherwise(F.col("b.VBELV")).alias("POSNV"),
        F.when(F.col("a.`Issue qty`").isNotNull(), F.col("a.`Issue qty`"))
         .otherwise(F.col("b.VBELV")).alias("Issue qty")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_issuedt_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc()
)

VBFA_IssueDt = (
    VBFA_raw
    .filter(
        F.col("VBTYP_N").isin("R", "h") &
        (F.col("STUFE") == "01") &
        (F.col("BWART") == "631")
    )
    .withColumn("Issue date", F.col("ERDAT"))
    .withColumn("rn", F.row_number().over(vbfa_issuedt_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "Issue date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_returnqty_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc()
)

VBFA_ReturnQty = (
    VBFA_raw
    .filter(
        F.col("VBTYP_N").isin("R", "h") &
        (F.col("STUFE") == "02")
    )
    .withColumn(
        "Return qty",
        F.when(F.col("BWART") == "632", F.col("RFMNG"))
         .otherwise(-F.col("RFMNG"))
    )
    .withColumn("rn", F.row_number().over(vbfa_returnqty_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "Return qty")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_returndt_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.col("ERDAT").desc()
)

VBFA_ReturnDt = (
    VBFA_raw
    .filter(
        F.col("VBTYP_N").isin("R", "h","B") &
        (F.col("STUFE") == "02") &
        (F.col("BWART") == "632")
    )
    .withColumn("Return date", F.col("ERDAT"))
    .withColumn("rn", F.row_number().over(vbfa_returndt_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "BWART", "ERDAT", "Return date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbfa_serial_window = Window.partitionBy("VBELV", "POSNV").orderBy(
    F.concat(F.col("ERDAT"), F.col("ERZET")).desc()
)

VBFA_SERIAL = (
    VBFA_raw
    .filter(
        (F.col("VBTYP_N") == "J") &
        (F.col("VBTYP_V") == "C") &
        (F.col("BWART") == "631")
    )
    .withColumn("rn", F.row_number().over(vbfa_serial_window))
    .filter(F.col("rn") == 1)
    .select("VBELV", "POSNV", "VBELN", "POSNN", "RFMNG", "ERDAT")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vbep_window = Window.partitionBy("VBELN", "POSNR").orderBy(
    "MANDT", "VBELN", "POSNR", "ETENR"
)

VBEP = (
    VBEP_raw
    .filter(F.col("WMENG") > 0)
    .withColumn("rn", F.row_number().over(vbep_window))
    .filter(F.col("rn") == 1)
    .select("VBELN", "POSNR", "EDATU")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

KB_VBAP = VBAP_raw.filter(F.col("MATNR") != "").alias("KB")
KA_VBAP = VBAP_raw.alias("KA")

VBAP = (
    KB_VBAP
    .join(
        KA_VBAP,
        (F.col("KB.VBELN") == F.col("KA.VGBEL")) &
        (F.col("KB.POSNR") == F.col("KA.POSNR")),
        how="left"
    )
    .select(
        F.col("KB.VBELN").alias("KB_VBELN"),
        F.col("KA.VBELN").alias("KA_VBELN"),
        F.col("KB.LGORT").alias("KB_LGORT"),
        F.col("KA.LGORT").alias("KA_LGORT"),
        F.col("KB.POSNR").alias("KB_POSNR"),
        F.col("KA.POSNR").alias("KA_POSNR"),
        F.col("KB.WERKS").alias("KB_WERKS"),
        F.col("KA.WERKS").alias("KA_WERKS"),
        F.col("KB.SPART").alias("KB_SPART"),
        F.col("KA.SPART").alias("KA_SPART"),
        F.col("KB.MATNR").alias("KB_MATNR"),
        F.col("KA.MATNR").alias("KA_MATNR"),
        F.col("KB.KWMENG").alias("KB_KWMENG"),
        F.col("KA.KWMENG").alias("KA_KWMENG"),
        F.col("KB.ABGRU").alias("KB_ABGRU"),
        F.col("KA.ABGRU").alias("KA_ABGRU"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Strip leading zeros from SERNR
sernr_stripped = F.expr(
    "SUBSTRING(SERNR, PATINDEX('%[^0]%', SERNR + '.'), LEN(SERNR))"
)

SerialNO = (
    SER01_raw
    .join(OBJK_raw, "OBKNR")
    .groupBy("LIEF_NR", "POSNR")
    .agg(
        F.array_join(
            F.array_sort(F.collect_list(
                F.expr("substring(SERNR, length(regexp_replace(SERNR, '^0+', '')) * -1)")
            )),
            ","
        ).alias("SERNR")
    )
)

# Simpler equivalent using regexp_replace to strip leading zeros
SerialNO = (
    SER01_raw
    .join(OBJK_raw, "OBKNR")
    .withColumn("SERNR_clean", F.regexp_replace(F.col("SERNR"), r"^0+", ""))
    .groupBy("LIEF_NR", "POSNR")
    .agg(
        F.concat_ws(",", F.sort_array(F.collect_list("SERNR_clean"))).alias("SERNR")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MARA = (
    MARA_raw
    .filter(F.col("INTEGRATION_INCLUDE") == "1")
    .select("MATNR", "PRDHA")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PA0001 = (
    PA0001_raw
    .filter(F.col("ENDDA") == "99991231")
    .select("ENAME", "PERNR")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def strip_zeros(col_name):
    return F.regexp_replace(F.col(col_name), r"^0+", "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sap_to_date(col_name):
    return F.when(
        F.col(col_name) == 0, F.lit("1900-01-01").cast("date")
    ).otherwise(
        F.to_date(F.col(col_name).cast("string"), "yyyyMMdd")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

return_dt_logic = when(col("vrdt.Return date").isNull() | (coalesce(col("vrq.Return qty"), lit(0)) != col("vbap.kb_kwmeng")), col("vbep.edatu")) \
                  .otherwise(col("vrdt.Return date"))
fallback_return_dt = coalesce(return_dt_logic, col("vbep_kb.edatu"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cons_qty_expr(kulab, issue_qty, return_qty):
    net = F.coalesce(issue_qty, F.lit(0)) - F.coalesce(return_qty, F.lit(0))
    return F.coalesce(
        F.when(kulab > 0,
            F.when(kulab < net, kulab.cast("decimal(15,2)"))
             .otherwise(net.cast("decimal(15,2)"))
        ).otherwise(kulab.cast("decimal(15,2)")),
        F.lit(0)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def unit_price(vprsv, stprs, verpr, peinh):
    return F.when(
        vprsv == "S", stprs / F.nullif(peinh, F.lit(0))
    ).otherwise(
        verpr / F.nullif(peinh, F.lit(0))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def strip_zeros(column_name):
    """Replicates SUBSTRING(col, PATINDEX('%[^0]%', col+'.'), LEN(col))"""
    return regexp_replace(col(column_name), "^0+", "")

def parse_date(col_name):
    """Replicates CASE WHEN erdat = 0 THEN '1900-01-01' ELSE CAST(...)"""
    return when(col(col_name) == lit('0'), to_date(lit('1900-01-01'))) \
           .otherwise(to_date(col(col_name).cast("string"), "yyyyMMdd"))

# Common logic components
net_issue = coalesce(col("issue qty"), lit(0)) - coalesce(col("return qty"), lit(0))
nullif_peinh = when(col("peinh") == 0, lit(None)).otherwise(col("peinh"))

def calc_cons_qty():
    return coalesce(
        when(col("kulab") > 0, 
             when(col("kulab") < net_issue, col("kulab").cast("decimal(15,2)"))
             .otherwise(net_issue.cast("decimal(15,2)")))
        .otherwise(col("kulab").cast("decimal(15,2)")), lit(0)
    )

def calc_cons_val():
    qty_multiplier = when(col("kulab") > 0, when(col("kulab") < net_issue, col("kulab")).otherwise(net_issue))
    return coalesce((
        when(col("vprsv") == 'S', (col("stprs") / nullif_peinh) * qty_multiplier)
        .otherwise((col("verpr") / nullif_peinh) * qty_multiplier)
    ).cast("decimal(18,2)"), lit(0))

def calc_value(price_control_col, price_s, price_v, peinh_col, qty_col):
    return coalesce((
        when(col(price_control_col) == 'S', col(price_s) / nullif_peinh * coalesce(col(qty_col), lit(0)))
        .otherwise(col(price_v) / nullif_peinh * coalesce(col(qty_col), lit(0)))
    ).cast("decimal(18,2)"), lit(0))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VBAK_CTE
vbak_cte = VBAK_raw.filter(col("auart").isin("KB", "ZKBB", "ZKDD","ZSOR","ZMOR")) \
    .select("vbeln", "kunnr", "augru", "ernam", "bstnk", "erdat", "waerk")

# MSKU_DATA
w_msku = Window.partitionBy("matnr", "werks", "kunnr").orderBy(concat(col("lfgja"), col("lfmon")).desc())
msku_data = MSKU_raw.withColumn("rn", row_number().over(w_msku)) \
    .filter(col("rn") == 1) \
    .select("matnr", "werks", "kunnr", "kulab", "lfgja")

# MAKT
w_makt = Window.partitionBy("matnr").orderBy("matnr")
makt_cte = MAKT_raw.filter(~col("maktx").like("%delete%")) \
    .withColumn("rn", row_number().over(w_makt)) \
    .filter(col("rn") == 1) \
    .select("matnr", col("maktx").alias("Material Text"))

# VBFA_Return_Order
vbfa_return_order = VBFA_raw.filter(col("vbtyp_n") == 'T') \
    .groupBy("vbelv", "posnv") \
    .agg(_max("erdat").alias("erdat"))

# VBFA_IssueQty_631
w_vbfa_issue = Window.partitionBy("vbelv", "posnv").orderBy(col("vbelv"), col("posnv"), col("erdat").desc())
vbfa_issueqty_631 = VBFA_raw.filter((col("vbtyp_n").isin('R', 'h')) & (col("stufe") == '01') & (col("bwart") == '631')) \
    .withColumn("rn", row_number().over(w_vbfa_issue)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", "erdat", col("rfmng").alias("Issue qty_631"))

# VBFA_IssueQty_NOT631
vbfa_issueqty_not631 = VBFA_raw.filter((col("vbtyp_n").isin('R', 'h')) & (col("stufe") == '01') & (col("bwart") != '631')) \
    .withColumn("rn", row_number().over(w_vbfa_issue)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", "erdat", (-col("rfmng")).alias("Issue qty_not631"))

# VBFA_IssueQty (Combining 631 and NOT 631)
vbfa_issueqty = vbfa_issueqty_631.alias("a").join(vbfa_issueqty_not631.alias("b"), 
    (col("a.vbelv") == col("b.vbelv")) & (col("a.posnv") == col("b.posnv")), "full") \
    .select(
        coalesce(col("a.vbelv"), col("b.vbelv")).alias("vbelv"),
        coalesce(col("a.posnv"), col("b.vbelv")).alias("posnv"), # Mapping exactly as written in SQL script
        coalesce(col("a.Issue qty_631"), col("b.vbelv")).alias("Issue qty") # Replicating the SQL CASE logic fallback
    )

# VBFA_IssueDt
vbfa_issuedt = VBFA_raw.filter((col("vbtyp_n").isin('R', 'h')) & (col("stufe") == '01') & (col("bwart") == '631')) \
    .withColumn("rn", row_number().over(w_vbfa_issue)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", col("erdat").alias("Issue date"))

# VBFA_ReturnQty
w_vbfa_return = Window.partitionBy("vbelv", "posnv").orderBy(col("vbelv"), col("posnv"), col("erdat").desc())
vbfa_returnqty = VBFA_raw.filter((col("vbtyp_n").isin('R', 'h')) & (col("stufe") == '02')) \
    .withColumn("rn", row_number().over(w_vbfa_return)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", when(col("bwart") == '632', col("rfmng")).otherwise(-col("rfmng")).alias("Return qty"))

# VBFA_ReturnDt
vbfa_returndt = VBFA_raw.filter((col("vbtyp_n").isin('R', 'h')) & (col("stufe") == '02') & (col("bwart") == '632')) \
    .withColumn("rn", row_number().over(w_vbfa_return)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", "bwart", col("erdat").alias("Return date"))

# VBFA_SERIAL
w_serial = Window.partitionBy("vbelv", "posnv").orderBy(concat(col("erdat"), col("erzet")).desc())
vbfa_serial = VBFA_raw.filter((col("vbtyp_n") == 'J') & (col("vbtyp_v") == 'C') & (col("bwart") == '631')) \
    .withColumn("rn", row_number().over(w_serial)) \
    .filter(col("rn") == 1) \
    .select("vbelv", "posnv", "vbeln", "posnn", "rfmng", "erdat")

# VBEP
w_vbep = Window.partitionBy("vbeln", "posnr").orderBy("mandt", "vbeln", "posnr", "etenr")
vbep_cte = VBEP_raw.filter(col("wmeng") > 0) \
    .withColumn("rn", row_number().over(w_vbep)) \
    .filter(col("rn") == 1) \
    .select("vbeln", "posnr", "edatu")

# VBAP (Self-join KB and KA)
kb_vbap = VBAP_raw.alias("kb_vbap").filter(col("matnr") != '')
ka_vbap = VBAP_raw.alias("ka_vbap")
vbap_cte = kb_vbap.join(ka_vbap, (col("kb_vbap.vbeln") == col("ka_vbap.vgbel")) & (col("kb_vbap.posnr") == col("ka_vbap.posnr")), "left") \
    .select(
        col("kb_vbap.vbeln").alias("kb_vbeln"), col("ka_vbap.vbeln").alias("ka_vbeln"),
        col("kb_vbap.lgort").alias("kb_lgort"), col("ka_vbap.lgort").alias("ka_lgort"),
        col("kb_vbap.posnr").alias("kb_posnr"), col("ka_vbap.posnr").alias("ka_posnr"),
        col("kb_vbap.werks").alias("kb_werks"), col("ka_vbap.werks").alias("ka_werks"),
        col("kb_vbap.spart").alias("kb_spart"), col("ka_vbap.spart").alias("ka_spart"),
        col("kb_vbap.matnr").alias("kb_matnr"), col("ka_vbap.matnr").alias("ka_matnr"),
        col("kb_vbap.kwmeng").alias("kb_kwmeng"), col("ka_vbap.kwmeng").alias("ka_kwmeng"),
        col("kb_vbap.abgru").alias("kb_abgru"), col("ka_vbap.abgru").alias("ka_abgru")
    )

# SerialNO
# In PySpark, sorting before collect_list guarantees the equivalent of `WITHIN GROUP (ORDER BY ...)`
# serialno_cte = SER01_raw.join(OBJK_raw, ser01["obknr"] == objk["obknr"]) \
#     .withColumn("stripped_sernr", regexp_replace(col("sernr"), "^0+", "")) \
#     .orderBy(col("lief_nr"), col("posnr").desc()) \
#     .groupBy("lief_nr", "posnr") \
#     .agg(concat_ws(",", collect_list("stripped_sernr")).alias("sernr"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

KA_Order = VBAK_raw.alias("KA_Order")
base_df = VBAK_raw.alias("vbak")

# 1. Join KA_Order
step1 = base_df.join(
    KA_Order,
    (F.col("KA_Order.KUNNR") == F.col("vbak.KUNNR")) &
    (F.col("KA_Order.VGBEL") == F.col("vbak.VBELN")),
    how="left"
)

step2 = step1.join(
    KNA1_raw.alias("kna1"),
    F.col("vbak.KUNNR") == F.col("kna1.KUNNR"),
    how="left"
)

step3 = step2.join(
    VBAP.alias("vbap"),
    (F.col("KA_Order.VBELN") == F.col("vbap.KA_VBELN")) &
    (F.col("vbak.VBELN") == F.col("vbap.KB_VBELN")),
    how="left"
)

step4 = step3.join(
    VBFA_Return_Order.alias("ret_ord"),
    (F.col("vbap.KB_VBELN") == F.col("ret_ord.VBELV")) &
    (F.col("vbap.KA_POSNR") == F.col("ret_ord.POSNV")),
    how="left"
)

step5 = step4.join(
    VBPA_SHIP.alias("ship"),
    (F.col("vbak.VBELN") == F.col("ship.VBELN")) &
    (F.col("ship.POSNR") == "000000"),
    how="left"
)

step6 = step5.join(
    KNA1_raw.alias("kna"),
    F.col("ship.KUNNR") == F.col("kna.KUNNR"),
    how="left"
)

step7 = step6.join(
    MSKU_DATA.alias("msku"),
    (F.col("vbap.KA_MATNR") == F.col("msku.MATNR")) &
    (F.col("vbak.KUNNR") == F.col("msku.KUNNR")) &
    (F.col("vbap.KA_WERKS") == F.col("msku.WERKS")),
    how="left"
)

step8 = step7.join(
    MARA.alias("mara"),
    F.col("mara.MATNR") == F.col("vbap.KA_MATNR"),
    how="left"
)

step9 = step8.join(
    MAKT.alias("makt"),
    F.col("makt.MATNR") == F.col("mara.MATNR"),
    how="left"
)


step10 = step9.join(
    VBPA_ZR.alias("zr"),
    F.col("zr.VBELN") == F.col("vbak.VBELN"),
    how="left"
)

step11 = step10.join(
    PA0001.alias("pa"),
    F.col("pa.PERNR") == F.col("zr.PERNR"),
    how="left"
)

# 12. Join VBFA_IssueQty
step12 = step11.join(
    VBFA_IssueQty.alias("iq"),
    (F.col("vbap.KB_VBELN") == F.col("iq.VBELV")) &
    (F.col("vbap.KA_POSNR") == F.col("iq.POSNV")),
    how="left"
)

# 13. Join VBFA_IssueDt
step13 = step12.join(
    VBFA_IssueDt.alias("id"),
    (F.col("vbap.KB_VBELN") == F.col("id.VBELV")) &
    (F.col("vbap.KA_POSNR") == F.col("id.POSNV")),
    how="left"
)

# 14. Join VBFA_ReturnQty
# step14 = step13.join(
#     VBFA_ReturnQty.alias("rq"),
#     (F.col("vbap.KB_VBELN") == F.col("rq.VBELV")) &
#     (F.col("vbap.KA_POSNR") == F.col("rq.POSNV")),
#     how="left"
# )

step14 = step13.join(
    VBFA_ReturnQty.alias("vrq"),  # Changed alias from "rq" to "vrq"
    (F.col("vbap.KB_VBELN") == F.col("vrq.VBELV")) &  # Updated to vrq
    (F.col("vbap.KA_POSNR") == F.col("vrq.POSNV")),   # Updated to vrq
    how="left"
)

# 15. Join VBFA_ReturnDt
step15 = step14.join(
    VBFA_ReturnDt.alias("vrdt"),  # Changed alias to vrdt
    (F.col("vbap.KB_VBELN") == F.col("vrdt.VBELV")) &  # Updated to vrdt
    (F.col("vbap.KA_POSNR") == F.col("vrdt.POSNV")),   # Updated to vrdt
    how="left"
)

# 16. Join MBEW_raw
step16 = step15.join(
    MBEW_raw.alias("mbew"),
    (F.col("mbew.MATNR") == F.col("vbap.KA_MATNR")) &
    (F.col("mbew.BWKEY") == F.col("vbap.KA_WERKS")) &
    (F.col("mbew.BWTAR") == ""),
    how="left"
)

# 17. Join VBFA_SERIAL
step17 = step16.join(
    VBFA_SERIAL.alias("vfs"),
    (F.col("vbap.KB_VBELN") == F.col("vfs.VBELV")) &
    (F.col("vbap.KA_POSNR") == F.col("vfs.POSNV")),
    how="left"
)

# 18. Join SerialNO
step18 = step17.join(
    SerialNO.alias("sno"),
    (F.col("sno.LIEF_NR") == F.col("vfs.VBELN")) &
    (F.col("sno.POSNR") == F.col("vfs.POSNN")),
    how="left"
)

# 19. Join VBEP
step19 = step18.join(
    VBEP.alias("vbep"),
    (F.col("vbep.VBELN") == F.col("KA_Order.VBELN")) &
    (F.col("vbap.KA_POSNR") == F.col("vbep.POSNR")),
    how="left"
)

# 20. Join VBEP (Second time)
step20 = step19.join(
    VBEP.alias("vbep_kb"),
    (F.col("vbep_kb.VBELN") == F.col("vbak.VBELN")) &
    (F.col("vbap.KA_POSNR") == F.col("vbep_kb.POSNR")),
    how="left"
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

part1_df = step20.select(
    strip_zeros("vbak.VBELN").alias("KB order"),
    strip_zeros("KA_Order.VBELN").alias("KA/ZR order"),
    strip_zeros("vbak.KUNNR").alias("Customer"),
    F.col("vbak.AUGRU").alias("Order reason"),
    F.col("kna1.NAME1").alias("Name 1"),
    F.col("kna1.ORT01").alias("City"),
    F.col("kna1.REGIO").alias("Region"),
    F.col("kna1.PSTLZ").alias("Postal Code"),
    F.col("kna.NAME1").alias("Ship-To Name"),
    F.col("vbap.KA_WERKS").alias("Plant"),
    F.col("vbap.KB_LGORT").alias("Stor. Location"),
    F.col("vbap.KA_SPART").alias("Product group"),
    strip_zeros("vbap.KA_MATNR").alias("Material"),
    
    # ADDED BACKTICKS: Columns with spaces must be escaped
    F.col("makt.`Material Text`").alias("Description"),
    
    strip_zeros("KA_Order.VBELN").alias("Sales Document"),
    strip_zeros("kna.KUNNR").alias("Ship-To Number"),
    strip_zeros("vbap.KA_POSNR").alias("Item"),
    F.col("mara.PRDHA").alias("Prod.hierarchy"),
    F.col("zr.PERNR").alias("Personnel No."),           
    F.col("pa.ENAME").alias("Empl./Appl.Name"),         
    F.col("vbak.ERNAM").alias("Created by_KB"),
    F.col("KA_Order.ERNAM").alias("Created by_KA"),
    F.col("vbak.BSTNK").alias("Customer Reference"),
    
    # NOTE: In your original code, this function was called `sap_to_date`. 
    # Ensure `parse_date` is defined or change it back to `sap_to_date`.
    parse_date("vbak.ERDAT").alias("KB Creation date"),
    parse_date("KA_Order.ERDAT").alias("KA Creation date"),
    
    F.concat(F.col("vbak.VBELN"), F.col("vbap.KA_POSNR"), F.col("KA_Order.VBELN")).alias("INTEGRATION_ID"),
    F.col("sno.SERNR").alias("Serial Number"),
    
    # Ensure these don't require arguments, otherwise pass them in like calc_cons_qty(args)
    calc_cons_qty().alias("Cons qty"),
    calc_cons_val().alias("Cons val"),
    
    # ADDED BACKTICKS for `Issue date`
    F.when(F.col("id.`Issue date`").isNotNull() & fallback_return_dt.isNotNull(),  
         F.datediff(fallback_return_dt, F.col("id.`Issue date`"))).otherwise(F.lit(None)).alias("Con Days"),
         
    # ADDED BACKTICKS for `Issue qty`
    F.when(F.col("vbap.KB_KWMENG") == F.coalesce(F.col("iq.`Issue qty`"), F.lit(0)), F.lit('CmpGI')).otherwise(F.lit('IncGI')).alias("Issue status"),
    F.coalesce(F.col("iq.`Issue qty`").cast("decimal(15,2)"), F.lit(0)).alias("Issue qty"),
    
    # Note: earlier your func was `unit_price`, assuming `calc_value` is defined correctly
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "iq.`Issue qty`").alias("Issue val"),
    F.to_date(F.col("id.`Issue date`").cast("string"), "yyyyMMdd").alias("Issue date"),
    F.col("vbap.KB_KWMENG").cast("decimal(15,2)").alias("Order qty"),
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "vbap.KB_KWMENG").alias("Order val"),
    
    # ADDED BACKTICKS for `Return qty`
    # F.when(F.col("vbap.KB_KWMENG") == F.coalesce(F.col("rq.`Return qty`"), F.lit(0)), F.lit('CmpGI')).otherwise(F.lit('IncGI')).alias("Return status"),
    # F.coalesce(F.col("rq.`Return qty`").cast("decimal(15,2)"), F.lit(0)).alias("Return qty"),
    
    # calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "rq.`Return qty`").alias("Return val"),
    F.when(F.col("vbap.KB_KWMENG") == F.coalesce(F.col("vrq.`Return qty`"), F.lit(0)), F.lit('CmpGI')).otherwise(F.lit('IncGI')).alias("Return status"),
    F.coalesce(F.col("vrq.`Return qty`").cast("decimal(15,2)"), F.lit(0)).alias("Return qty"),
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "vrq.`Return qty`").alias("Return val"),
    F.to_date(fallback_return_dt.cast("string"), "yyyyMMdd").alias("Return date"),
    F.col("vbak.WAERK").alias("Currency")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 0. Initial Setup
base_df = VBAK_raw.alias("vbak")

# 1. Join KNA1 (Customer Master)
step1 = base_df.join(
    KNA1_raw.alias("kna1"), 
    F.col("vbak.KUNNR") == F.col("kna1.KUNNR"), 
    how="left"
)

# 2. Join VBAP (Sales Item Data)
step2 = step1.join(
    VBAP.alias("vbap"), 
    F.col("vbap.KA_VBELN").isNull() & (F.col("vbap.KB_VBELN") == F.col("vbak.VBELN")), 
    how="left"
)

# 3. Join VBFA Return Order
step3 = step2.join(
    VBFA_Return_Order.alias("vro"), 
    (F.col("vbap.KB_VBELN") == F.col("vro.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("vro.POSNV")), 
    how="left"
)

# 4. Join VBPA Ship To
step4 = step3.join(
    VBPA_SHIP.alias("vst"), 
    (F.col("vbak.VBELN") == F.col("vst.VBELN")) & 
    (F.col("vst.POSNR") == "000000"), 
    how="left"
)

# 5. Join KNA1 (Ship-To Customer Master)
step5 = step4.join(
    KNA1_raw.alias("kna"), 
    F.col("vst.KUNNR") == F.col("kna.KUNNR"), 
    how="left"
)

# 6. Join MSKU_DATA (Special Stocks)
step6 = step5.join(
    MSKU_DATA.alias("msku"), 
    (F.col("vbap.KB_MATNR") == F.col("msku.MATNR")) & 
    (F.col("vbak.KUNNR") == F.col("msku.KUNNR")) & 
    (F.col("vbap.KB_WERKS") == F.col("msku.WERKS")), 
    how="left"
)

# 7. Join MARA (Material Data)
step7 = step6.join(
    MARA.alias("mara"), 
    F.col("mara.MATNR") == F.col("vbap.KB_MATNR"), 
    how="left"
)

# 8. Join MAKT (Material Descriptions)
step8 = step7.join(
    MAKT.alias("makt"), 
    F.col("makt.MATNR") == F.col("mara.MATNR"), 
    how="left"
)

# 9. Join VBPA ZR (Partner Data)
step9 = step8.join(
    VBPA_ZR.alias("vpz"), 
    F.col("vpz.VBELN") == F.col("vbak.VBELN"), 
    how="left"
)

# 10. Join PA0001 (HR Master Record)
step10 = step9.join(
    PA0001.alias("pa0001"), 
    F.col("pa0001.PERNR") == F.col("vpz.PERNR"), 
    how="left"
)

# 11. Join VBFA Issue Qty
step11 = step10.join(
    VBFA_IssueQty.alias("viq"), 
    (F.col("vbap.KB_VBELN") == F.col("viq.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("viq.POSNV")), 
    how="left"
)

# 12. Join VBFA Issue Date
step12 = step11.join(
    VBFA_IssueDt.alias("vidt"), 
    (F.col("vbap.KB_VBELN") == F.col("vidt.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("vidt.POSNV")), 
    how="left"
)

# 13. Join VBFA Return Qty
step13 = step12.join(
    VBFA_ReturnQty.alias("vrq"), 
    (F.col("vbap.KB_VBELN") == F.col("vrq.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("vrq.POSNV")), 
    how="left"
)

# 14. Join VBFA Return Date
step14 = step13.join(
    VBFA_ReturnDt.alias("vrdt"), 
    (F.col("vbap.KB_VBELN") == F.col("vrdt.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("vrdt.POSNV")), 
    how="left"
)

# 15. Join MBEW (Material Valuation)
step15 = step14.join(
    MBEW_raw.alias("mbew"), 
    (F.col("mbew.MATNR") == F.col("vbap.KB_MATNR")) & 
    (F.col("mbew.BWKEY") == F.col("vbap.KB_WERKS")) & 
    (F.col("mbew.BWTAR") == ""), 
    how="left"
)

# 16. Join VBFA Serial
step16 = step15.join(
    VBFA_SERIAL.alias("vfs"), 
    (F.col("vbap.KB_VBELN") == F.col("vfs.VBELV")) & 
    (F.col("vbap.KB_POSNR") == F.col("vfs.POSNV")), 
    how="left"
)

# 17. Join SerialNO
step17 = step16.join(
    SerialNO.alias("sno"), 
    (F.col("sno.LIEF_NR") == F.col("vfs.VBELN")) & 
    (F.col("sno.POSNR") == F.col("vfs.POSNN")), 
    how="left"
)

# 18. Join VBEP (Schedule Line Data)
step18 = step17.join(
    VBEP.alias("vbep"), 
    (F.col("vbep.VBELN") == F.col("vbak.VBELN")) & 
    (F.col("vbap.KB_POSNR") == F.col("vbep.POSNR")), 
    how="left"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

part2_df = step18.select(
    strip_zeros("vbak.VBELN").alias("KB order"),
    F.lit(None).cast("string").alias("KA/ZR order"),
    strip_zeros("vbak.KUNNR").alias("Customer"),
    F.col("vbak.AUGRU").alias("Order reason"),
    F.col("kna1.NAME1").alias("Name 1"),
    F.col("kna1.ORT01").alias("City"),
    F.col("kna1.REGIO").alias("Region"),
    F.col("kna1.PSTLZ").alias("Postal Code"),
    F.col("kna.NAME1").alias("Ship-To Name"),
    F.col("vbap.KB_WERKS").alias("Plant"),
    F.col("vbap.KB_LGORT").alias("Stor. Location"),
    F.col("vbap.KB_SPART").alias("Product group"),
    strip_zeros("vbap.KB_MATNR").alias("Material"),
    
    # Backticks for columns with spaces
    F.col("makt.`Material Text`").alias("Description"),
    
    F.lit(None).cast("string").alias("Sales Document"),
    strip_zeros("kna.KUNNR").alias("Ship-To Number"),
    strip_zeros("vbap.KB_POSNR").alias("Item"),
    F.col("mara.PRDHA").alias("Prod.hierarchy"),
    F.col("vpz.PERNR").alias("Personnel No."),
    F.col("pa0001.ENAME").alias("Empl./Appl.Name"),
    F.col("vbak.ERNAM").alias("Created by_KB"),
    F.lit(None).cast("string").alias("Created by_KA"),
    F.col("vbak.BSTNK").alias("Customer Reference"),
    parse_date("vbak.ERDAT").alias("KB Creation date"),
    F.lit(None).cast("date").alias("KA Creation date"),
    F.concat(F.col("vbak.VBELN"), F.col("vbap.KB_POSNR"), F.col("vbak.VBELN")).alias("INTEGRATION_ID"),
    F.col("sno.SERNR").alias("Serial Number"),
    calc_cons_qty().alias("Cons qty"),
    calc_cons_val().alias("Cons val"),
    F.lit(0).alias("Con Days"),
    
    F.when(F.col("vbap.KB_KWMENG") == F.coalesce(F.col("viq.`Issue qty`"), F.lit(0)), F.lit("CmpGI")).otherwise(F.lit("IncGI")).alias("Issue status"),
    F.coalesce(F.col("viq.`Issue qty`").cast("decimal(15,2)"), F.lit(0)).alias("Issue qty"),
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "viq.`Issue qty`").alias("Issue val"),
    
    F.to_date(F.col("vidt.`Issue date`").cast("string"), "yyyyMMdd").alias("Issue date"),
    
    F.col("vbap.KB_KWMENG").cast("decimal(15,2)").alias("Order qty"),
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "vbap.KB_KWMENG").alias("Order val"),
    
    F.lit(None).cast("string").alias("Return status"),
    F.coalesce(F.col("vrq.`Return qty`").cast("decimal(15,2)"), F.lit(0)).alias("Return qty"),
    calc_value("mbew.VPRSV", "mbew.STPRS", "mbew.VERPR", "mbew.PEINH", "vrq.`Return qty`").alias("Return val"),
    
    F.lit(None).cast("date").alias("Return date"),
    F.col("vbak.WAERK").alias("Currency")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = part1_df.unionByName(part2_df)

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

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Silver_LH.analytics.spares_consignment_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
