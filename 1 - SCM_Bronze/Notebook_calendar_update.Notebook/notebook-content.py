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

# =========================
# CONFIG
# =========================
start_date = "2017-01-01"

# =========================
# DATE SEQUENCE (1 YEAR)
# =========================


df = spark.sql(f"""
SELECT explode(sequence(
    to_date('{start_date}'),
    to_date('2030-12-31')
)) AS Date
""")


# =========================
# BASE CALCULATIONS
# =========================
df = df.withColumn("fiscal_day_of_week", (F.dayofweek("Date") % 7) + 1)

df = df.withColumn(
    "fiscal_week",
    ((F.datediff("Date", F.lit(start_date))) / 7 + 1).cast("int")
)

df = df.withColumn("sap_fiscal_week", F.col("fiscal_week"))

# =========================
# CALENDAR ATTRIBUTES
# =========================
df = df.withColumn("date_key", F.date_format("Date", "yyyyMMdd").cast("int"))

df = df.withColumn("day", F.dayofmonth("Date"))

df = df.withColumn(
    "day_suffix",
    F.when(F.col("day").isin(11,12,13), "th")
     .when(F.col("day") % 10 == 1, "st")
     .when(F.col("day") % 10 == 2, "nd")
     .when(F.col("day") % 10 == 3, "rd")
     .otherwise("th")
)

df = df.withColumn("weekday", (F.dayofweek("Date") % 7) + 1)
df = df.withColumn("weekday_name", F.date_format("Date", "EEEE"))
df = df.withColumn("weekday_short", F.upper(F.date_format("Date", "EEE")))
df = df.withColumn("weekday_first_letter", F.substring(F.date_format("Date", "EEEE"), 1, 1))

df = df.withColumn("day_of_year", F.dayofyear("Date"))
df = df.withColumn("week_of_year", F.weekofyear("Date"))

# ✅ FIXED week_of_month (no error now)
df = df.withColumn(
    "week_of_month",
    F.ceil(F.dayofmonth("Date") / 7.0)
)

df = df.withColumn("month", F.month("Date"))
df = df.withColumn("month_name", F.date_format("Date", "MMMM"))
df = df.withColumn("month_short", F.upper(F.date_format("Date", "MMM")))
df = df.withColumn("month_first_letter", F.substring(F.date_format("Date", "MMMM"), 1, 1))

df = df.withColumn("quarter", F.quarter("Date"))

df = df.withColumn(
    "quarter_name",
    F.when(F.col("quarter") == 1, "First")
     .when(F.col("quarter") == 2, "Second")
     .when(F.col("quarter") == 3, "Third")
     .otherwise("Fourth")
)

df = df.withColumn("year", F.year("Date"))

df = df.withColumn("mmyyyy", F.date_format("Date", "MMyyyy"))

df = df.withColumn(
    "year_month",
    F.concat(F.year("Date"), F.upper(F.date_format("Date", "MMM")))
)

df = df.withColumn(
    "is_weekend",
    F.when(F.dayofweek("Date").isin(1,7), 1).otherwise(0)
)

df = df.withColumn("special_days", F.lit(None))

# =========================
# DATE BOUNDARIES
# =========================
df = df.withColumn("first_date_year", F.trunc("Date", "year"))
df = df.withColumn("last_date_year", F.last_day(F.add_months(F.trunc("Date", "year"), 11)))

df = df.withColumn("first_date_quarter", F.trunc("Date", "quarter"))
df = df.withColumn("last_date_quarter", F.last_day(F.add_months(F.trunc("Date", "quarter"), 2)))

df = df.withColumn("first_date_month", F.trunc("Date", "month"))
df = df.withColumn("last_date_month", F.last_day("Date"))

df = df.withColumn("first_date_week", F.date_sub("Date", F.dayofweek("Date") - 1))
df = df.withColumn("last_date_week", F.date_add("Date", 7 - F.dayofweek("Date")))

# =========================
# FISCAL YEAR
# =========================
df = df.withColumn(
    "fiscal_year",
    F.when(F.month("Date") >= 4, F.year("Date"))
     .otherwise(F.year("Date") + 1)
)

# =========================
# FINAL CALCULATIONS
# =========================
df = df.withColumn(
    "fiscal_date_ymw",
    F.concat(
        F.col("fiscal_year"),
        F.lit("-"),
        F.col("month_short"),
        F.lit("-W"),
        F.col("fiscal_week")
    )
)

df = df.withColumn("sap_date", F.add_months("Date", 12))

# =========================
# HOLIDAY JOIN (FIXED)
# =========================
holidays_df = spark.table("calendar_holidays").alias("h")
df = df.alias("d")

df = df.join(
    holidays_df,
    df["Date"] == holidays_df["holiday_date"],
    "left"
).select(
    "d.*",
    F.when(F.col("h.is_holiday") == 1, 1).otherwise(0).alias("is_holiday"),
    F.when(F.col("h.is_holiday") == 1, F.col("h.holiday_name")).otherwise("").alias("holiday_name")
)

# =========================
# CREATE VIEW
# =========================
df.write.mode("overwrite").saveAsTable("dbo.calendar_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
