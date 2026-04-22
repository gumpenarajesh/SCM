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
from pyspark.sql.functions import (
    col, lit, when, concat, expr,
    dayofyear, dayofmonth, month, year,
    quarter, date_add, last_day,
    weekofyear, date_format,lpad,max,col
)
from pyspark.sql.types import IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr, to_date

tcurr_df = spark.table("SCM_Bronze_LH.sap.tcurr") \
    .filter(col("MANDT") == "100")

tcurr_transformed = (
    tcurr_df
    .select(
        col("MANDT").alias("Client"),
        col("KURST").alias("Exchange_Rate_Type"),
        col("FCURR").alias("From_currency"),
        col("TCURR").alias("To_currency"),
        
        date_format(
            to_date(
                expr("CAST(99999999 - CAST(GDATU AS INT) AS STRING)"),
                "yyyyMMdd"
            ),
            "yyyyMM"
        ).alias("Exchange_Rate_YearMonth"),
        
        col("UKURS").alias("Exchange_Rate"),
        col("FFACT").alias("From_Currency_Units"),
        col("TFACT").alias("To_Currency_Units")
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar = spark.table("SCM_Bronze_LH.dbo.Calender")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar = spark.table("SCM_Bronze_LH.dbo.Calender").select(
    col("Date"),
    col("Fiscal_Year").alias("Fiscal Year"),
    col("Month").alias("Fiscal Period"),
    col("Fiscal_Year/Period").alias("Fiscal Year/Period")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar = calendar.withColumn(
    "year_month",
    date_format(col("Date"), "yyyyMM")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(calendar)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tcurr_with_fiscal = (
    tcurr_transformed.alias("t")
    .join(
        calendar.alias("c"),
        col("t.Exchange_Rate_YearMonth") == col("c.year_month"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tcurr_with_fiscal)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tcurr_with_fiscal.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df_0= (
    tcurr_with_fiscal
    .groupBy("Exchange_Rate_YearMonth")
    .agg(
        max(col("Exchange_Rate")).alias("Max_Exchange_Rate")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_df = (
    tcurr_with_fiscal.alias("t")
    .join(
        max_df.alias("m"),
        (
            (col("t.Exchange_Rate_YearMonth") == col("m.Exchange_Rate_YearMonth")) &
            (col("t.Exchange_Rate") == col("m.Max_Exchange_Rate"))
        ),
        "inner"
    )
    .select(
        "t.Client",
        "t.Exchange_Rate_Type",
        "t.From_currency",
        "t.To_currency",
        "t.Exchange_Rate_YearMonth",
        "t.Exchange_Rate",
        "t.From_Currency_Units",
        "t.To_Currency_Units",
        "t.Date",
        "t.Fiscal Year",
        "t.Fiscal Period",
        "t.Fiscal Year/Period",
        "t.year_month"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

window_spec = Window.partitionBy("Exchange_Rate_YearMonth").orderBy(col("Exchange_Rate").desc())

result_df_test = (
    tcurr_with_fiscal
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)  
    .drop("rn")
    .select(
        "Client",
        "Exchange_Rate_Type",
        "From_currency",
        "To_currency",
        "Exchange_Rate_YearMonth",
        "Exchange_Rate",
        "From_Currency_Units",
        "To_Currency_Units",
        "Date",
        "Fiscal Year",
        "Fiscal Period",
        "Fiscal Year/Period",
        "year_month"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import max

# window_spec = Window.partitionBy("Exchange_Rate_YearMonth")

# resu = (
#     tcurr_with_fiscal
#     .withColumn("max_rate", max("Exchange_Rate").over(window_spec))
#     .filter(col("Exchange_Rate") == col("max_rate"))
#     .drop("max_rate")
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result_df_0.filter(
    col("Exchange_Rate_YearMonth").startswith("2018")
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    result_df_1_1.filter(
        (col("From_currency") == "GBP") & 
        (col("Exchange_Rate_YearMonth").startswith("2018"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tcurr_with_fiscal.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tcurr_with_fiscal.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy(
    "From_currency", "To_currency", "Exchange_Rate_Type"
).orderBy(col("Exchange_Rate_Date").desc())

previous_period_rate = (
    tcurr_with_fiscal
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(col("rn") == 2)   # previous record
    .select(
        col("Fiscal Year/Period"),
        col("From_currency"),
        col("To_currency"),
        col("Exchange_Rate").alias("Prev_Exchange_Rate")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constant_currency = (
    tcurr_with_fiscal.alias("t")
    .join(
        previous_period_rate.alias("p"),
        (col("t.From_currency") == col("p.From_currency")) &
        (col("t.To_currency") == col("p.To_currency")),
        "left"
    )
    .select(
        col("t.Fiscal Year/Period"),
        col("t.Fiscal Period"),
        col("t.Fiscal Year"),
        col("t.From_currency").alias("Currency key"),
        
        col("p.Prev_Exchange_Rate").alias("BW_P"),
        col("t.Exchange_Rate").alias("BW_V"),
        
        col("t.Exchange_Rate").alias("Rate (P)"),
        col("t.Exchange_Rate").alias("Rate (V)")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = constant_currency.select(
    col("Fiscal Year/Period"),
    col("Fiscal Period"),
    col("Fiscal Year").cast("string"),
    col("Currency key"),
    col("BW_P"),
    col("BW_V"),
    col("Rate (P)"),
    col("Rate (V)")
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

final_df = final_df.withColumn(
    "Posting period",
    lpad(col("Fiscal Period").cast("string"), 3, "0")
).drop("Fiscal Period")

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

final_df = final_df.select(
    "Fiscal year/period",
    "Posting period",
    "Fiscal year",
    "Currency key",
    "BW_P",
    "BW_V",
    "Rate (P)",
    "Rate (V)"
)

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

final_df = final_df.withColumnRenamed("Fiscal_year/period", "Fiscal_year|period")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = final_df.withColumnRenamed("Rate_(P)", "Rate_P")
final_df = final_df.withColumnRenamed("Rate_(V)", "Rate_V")

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

filtered_final_df=final_df.dropna()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_final_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(filtered_final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_final_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("SCM_Silver_LH.analytics.currency_exchange_rates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
