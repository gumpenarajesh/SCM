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

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, concat, expr,
    dayofyear, dayofmonth, month, year,
    quarter, date_add, last_day,
    weekofyear, date_format,lpad
)
from pyspark.sql.types import IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "2017-01-01"
end_date = "2030-12-31"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date

start_dt = date.fromisoformat(start_date)
end_dt = date.fromisoformat(end_date)
total_days = (end_dt - start_dt).days

seq_df = spark.range(0, total_days + 1).toDF("n")

d = (
    seq_df
    .withColumn("Date", expr(f"date_add(to_date('{start_date}'), cast(n as int))"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = (
    d
    .withColumn("Date Key",
                year(col("Date")) * 10000 + month(col("Date")) * 100 + dayofmonth(col("Date")))

    .withColumn("Day", dayofmonth(col("Date")))

    .withColumn("Day Suffix",
                when(dayofmonth(col("Date")).isin(11, 12, 13), lit("th"))
                .when(dayofmonth(col("Date")) % 10 == 1, lit("st"))
                .when(dayofmonth(col("Date")) % 10 == 2, lit("nd"))
                .when(dayofmonth(col("Date")) % 10 == 3, lit("rd"))
                .otherwise(lit("th")))

    .withColumn("Weekday",
                (F.dayofweek(col("Date")) % 7 + 1).cast(IntegerType()))

    .withColumn("Weekday Name", date_format(col("Date"), "EEEE"))

    .withColumn("Weekday Short",
                F.upper(F.substring(date_format(col("Date"), "EEEE"), 1, 3)))

    .withColumn("Weekday First Letter",
                F.substring(date_format(col("Date"), "EEEE"), 1, 1))

    .withColumn("Day Of Year", dayofyear(col("Date")))

    .withColumn("Week Of Month",
                (F.floor(
                    F.datediff(col("Date"), F.trunc(col("Date"), "MM")) / 7
                ) + 1))

    .withColumn("Week Of Year", weekofyear(col("Date")))

    .withColumn("Month", month(col("Date")))

    .withColumn("Month Name", date_format(col("Date"), "MMMM"))

    .withColumn("Month Short",
                F.upper(F.substring(date_format(col("Date"), "MMMM"), 1, 3)))

    .withColumn("Month First Letter",
                F.substring(date_format(col("Date"), "MMMM"), 1, 1))

    .withColumn("Quarter", quarter(col("Date")))

    .withColumn("Quarter Name",
                when(quarter(col("Date")) == 1, lit("First"))
                .when(quarter(col("Date")) == 2, lit("Second"))
                .when(quarter(col("Date")) == 3, lit("Third"))
                .otherwise(lit("Fourth")))

    .withColumn("Year", year(col("Date")))

    .withColumn("MMYYYY",
                concat(
                    F.lpad(month(col("Date")).cast("string"), 2, "0"),
                    year(col("Date")).cast("string")
                ))

    .withColumn("Year Month",
                concat(
                    year(col("Date")).cast("string"),
                    F.upper(F.substring(date_format(col("Date"), "MMMM"), 1, 3))
                ))

    .withColumn("Is Weekend",
                when(F.dayofweek(col("Date")).isin(1, 7), lit(1)).otherwise(lit(0)))

    .withColumn("Special Days", lit(None).cast(StringType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = (
    src
    .withColumn("First Date Year",
                F.to_date(concat(year(col("Date")).cast("string"), lit("-01-01"))))

    .withColumn("Last Date Year",
                F.to_date(concat(year(col("Date")).cast("string"), lit("-12-31"))))

    .withColumn("First Date Quarter",
                F.to_date(concat(
                    year(col("Date")).cast("string"), lit("-"),
                    F.lpad(((quarter(col("Date")) - 1) * 3 + 1).cast("string"), 2, "0"),
                    lit("-01")
                )))

    .withColumn("Last Date Quarter",
                F.date_add(
                    F.add_months(
                        F.to_date(concat(year(col("Date")).cast("string"), lit("-01-01"))),
                        quarter(col("Date")) * 3
                    ), -1
                ))

    .withColumn("First Date Month", F.trunc(col("Date"), "MM"))

    .withColumn("Last Date Month", last_day(col("Date")))

    .withColumn("First Date Week",
                F.date_sub(col("Date"), (F.dayofweek(col("Date")) + 5) % 7))

    .withColumn("Last Date Week",
                F.date_add(col("Date"), 6 - ((F.dayofweek(col("Date")) + 5) % 7)))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = (
    src

    # CORE RULE: YEAR + 1
    .withColumn("Fiscal Year",
                year(col("Date")) + 1)

    .withColumn("Fiscal Month",
                month(col("Date")))

    .withColumn("Fiscal Quarter",
                quarter(col("Date")))

    .withColumn("Fiscal Week",
                weekofyear(col("Date")))

    .withColumn("SAP Fiscal Week",
                col("Fiscal Week"))

    .withColumn("Fiscal day of week",
                ((F.dayofweek(col("Date")) + 5) % 7 + 1).cast(IntegerType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar_holidays = spark.table("dbo.calendar_holidays")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = (
    src.alias("t1")
    .join(calendar_holidays.alias("t3"),
          col("t1.Date") == col("t3.holiday_date"),
          "left")

    .withColumn("Fiscal date ymw",
                concat(
                    col("t1.Fiscal Year").cast("string"), lit("-"),
                    col("t1.Month Short"), lit("-W"),
                    col("t1.Fiscal Week").cast("string")
                ))

    .withColumn("SAP Date",
                F.add_months(col("t1.Date"), 12))

    .withColumn("Is Holiday",
                when(col("t3.is_holiday") == 1, lit(1)).otherwise(lit(0)))

    .withColumn("Holiday Name",
                when(col("t3.is_holiday") == 1, col("t3.holiday_name"))
                .otherwise(lit("")))
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

calendar_final = final_df.withColumn(
    "Fiscal Year/Period",
    concat(
        col("Fiscal Year").cast("string"),
        lpad(col("Month").cast("string"), 2, "0")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_1 = calendar_final.select(
    "Date Key", "Date", "Day", "Day Suffix",
    "Weekday", "Weekday Name", "Weekday Short", "Weekday First Letter",
    "Day Of Year", "Week Of Month", "Week Of Year",
    "Month", "Month Name", "Month Short", "Month First Letter",
    "Quarter", "Quarter Name", "Year",
    "MMYYYY", "Year Month", "Is Weekend", "Special Days",
    "First Date Year", "Last Date Year",
    "First Date Quarter", "Last Date Quarter",
    "First Date Month", "Last Date Month",
    "First Date Week", "Last Date Week",
    "Fiscal Week", "Fiscal day of week", "SAP Fiscal Week",
    "Fiscal Year", "Fiscal date ymw",
    "SAP Date", "Is Holiday", "Holiday Name","Fiscal Year/Period"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.filter(col("Date") == "2026-03-23").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in final_df_1.columns:
    clean_name = c.strip().replace(" ", "_")
    final_df_1 = final_df_1.withColumnRenamed(c, clean_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_1.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_fixed = final_df.withColumn(
    "Week_Of_Month",
    col("Week_Of_Month").cast("double")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_1.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Bronze_LH.dbo.calender")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_fixed.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df_fixed.count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df_fixed)

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
