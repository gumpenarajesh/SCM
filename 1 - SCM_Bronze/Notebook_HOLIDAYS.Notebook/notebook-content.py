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
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

schema = StructType([
    StructField("holiday_date", DateType(), False),
    StructField("is_holiday", IntegerType(), False),
    StructField("holiday_name", StringType(), False),
])

# rows = [
#     ("2017-01-02", 1, "New Year's Day (Observed)"),
#     ("2017-01-16", 1, "Martin Luther King Jr. Day"),
#     ("2017-02-20", 1, "Washington's Birthday / Presidents Day"),
#     ("2017-05-29", 1, "Memorial Day"),
#     ("2017-07-04", 1, "Independence Day"),
#     ("2017-09-04", 1, "Labor Day"),
#     ("2017-10-09", 1, "Columbus Day"),
#     ("2017-11-10", 1, "Veterans Day (Observed)"),
#     ("2017-11-23", 1, "Thanksgiving Day"),
#     ("2017-12-25", 1, "Christmas Day"),
# ]

rows = [
("2017-01-02", 1, "New Year's Day (Observed)"),
("2017-01-16", 1, "Martin Luther King Jr. Day"),
("2017-02-20", 1, "Washington's Birthday / Presidents Day"),
("2017-05-29", 1, "Memorial Day"),
("2017-07-04", 1, "Independence Day"),
("2017-09-04", 1, "Labor Day"),
("2017-10-09", 1, "Columbus Day"),
("2017-11-10", 1, "Veterans Day (Observed)"),
("2017-11-23", 1, "Thanksgiving Day"),
("2017-12-25", 1, "Christmas Day"),

("2018-01-01", 1, "New Year's Day (Observed)"),
("2018-01-15", 1, "Martin Luther King Jr. Day"),
("2018-02-19", 1, "Washington's Birthday / Presidents Day"),
("2018-05-28", 1, "Memorial Day"),
("2018-07-04", 1, "Independence Day"),
("2018-09-03", 1, "Labor Day"),
("2018-10-08", 1, "Columbus Day"),
("2018-11-12", 1, "Veterans Day (Observed)"),
("2018-11-22", 1, "Thanksgiving Day"),
("2018-12-25", 1, "Christmas Day"),

("2019-01-01", 1, "New Year's Day (Observed)"),
("2019-01-21", 1, "Martin Luther King Jr. Day"),
("2019-02-18", 1, "Washington's Birthday / Presidents Day"),
("2019-05-27", 1, "Memorial Day"),
("2019-07-04", 1, "Independence Day"),
("2019-09-02", 1, "Labor Day"),
("2019-10-14", 1, "Columbus Day"),
("2019-11-11", 1, "Veterans Day (Observed)"),
("2019-11-28", 1, "Thanksgiving Day"),
("2019-12-25", 1, "Christmas Day"),

("2020-01-01", 1, "New Year's Day (Observed)"),
("2020-01-20", 1, "Martin Luther King Jr. Day"),
("2020-02-17", 1, "Washington's Birthday / Presidents Day"),
("2020-05-25", 1, "Memorial Day"),
("2020-07-04", 1, "Independence Day"),
("2020-09-07", 1, "Labor Day"),
("2020-10-12", 1, "Columbus Day"),
("2020-11-11", 1, "Veterans Day (Observed)"),
("2020-11-26", 1, "Thanksgiving Day"),
("2020-12-25", 1, "Christmas Day"),

("2021-01-01", 1, "New Year's Day (Observed)"),
("2021-01-18", 1, "Martin Luther King Jr. Day"),
("2021-02-15", 1, "Washington's Birthday / Presidents Day"),
("2021-05-31", 1, "Memorial Day"),
("2021-07-04", 1, "Independence Day"),
("2021-09-06", 1, "Labor Day"),
("2021-10-11", 1, "Columbus Day"),
("2021-11-11", 1, "Veterans Day (Observed)"),
("2021-11-25", 1, "Thanksgiving Day"),
("2021-12-25", 1, "Christmas Day"),

("2022-01-01", 1, "New Year's Day (Observed)"),
("2022-01-17", 1, "Martin Luther King Jr. Day"),
("2022-02-21", 1, "Washington's Birthday / Presidents Day"),
("2022-05-30", 1, "Memorial Day"),
("2022-07-04", 1, "Independence Day"),
("2022-09-05", 1, "Labor Day"),
("2022-10-10", 1, "Columbus Day"),
("2022-11-11", 1, "Veterans Day (Observed)"),
("2022-11-24", 1, "Thanksgiving Day"),
("2022-12-25", 1, "Christmas Day"),

("2023-01-02", 1, "New Year's Day (Observed)"),
("2023-01-16", 1, "Martin Luther King Jr. Day"),
("2023-02-20", 1, "Washington's Birthday / Presidents Day"),
("2023-05-29", 1, "Memorial Day"),
("2023-07-04", 1, "Independence Day"),
("2023-09-04", 1, "Labor Day"),
("2023-10-09", 1, "Columbus Day"),
("2023-11-11", 1, "Veterans Day (Observed)"),
("2023-11-23", 1, "Thanksgiving Day"),
("2023-12-25", 1, "Christmas Day"),

("2024-01-01", 1, "New Year's Day (Observed)"),
("2024-01-15", 1, "Martin Luther King Jr. Day"),
("2024-02-19", 1, "Washington's Birthday / Presidents Day"),
("2024-05-27", 1, "Memorial Day"),
("2024-07-04", 1, "Independence Day"),
("2024-09-02", 1, "Labor Day"),
("2024-10-14", 1, "Columbus Day"),
("2024-11-11", 1, "Veterans Day (Observed)"),
("2024-11-28", 1, "Thanksgiving Day"),
("2024-12-25", 1, "Christmas Day"),

("2025-01-01", 1, "New Year's Day (Observed)"),
("2025-01-20", 1, "Martin Luther King Jr. Day"),
("2025-02-17", 1, "Washington's Birthday / Presidents Day"),
("2025-05-26", 1, "Memorial Day"),
("2025-07-04", 1, "Independence Day"),
("2025-09-01", 1, "Labor Day"),
("2025-10-13", 1, "Columbus Day"),
("2025-11-11", 1, "Veterans Day (Observed)"),
("2025-11-27", 1, "Thanksgiving Day"),
("2025-12-25", 1, "Christmas Day"),

("2026-01-01", 1, "New Year's Day (Observed)"),
("2026-01-19", 1, "Martin Luther King Jr. Day"),
("2026-02-16", 1, "Washington's Birthday / Presidents Day"),
("2026-05-25", 1, "Memorial Day"),
("2026-07-04", 1, "Independence Day"),
("2026-09-07", 1, "Labor Day"),
("2026-10-12", 1, "Columbus Day"),
("2026-11-11", 1, "Veterans Day (Observed)"),
("2026-11-26", 1, "Thanksgiving Day"),
("2026-12-25", 1, "Christmas Day"),

("2027-01-01", 1, "New Year's Day (Observed)"),
("2027-01-18", 1, "Martin Luther King Jr. Day"),
("2027-02-15", 1, "Washington's Birthday / Presidents Day"),
("2027-05-31", 1, "Memorial Day"),
("2027-07-04", 1, "Independence Day"),
("2027-09-06", 1, "Labor Day"),
("2027-10-11", 1, "Columbus Day"),
("2027-11-11", 1, "Veterans Day (Observed)"),
("2027-11-25", 1, "Thanksgiving Day"),
("2027-12-25", 1, "Christmas Day"),

("2028-01-01", 1, "New Year's Day (Observed)"),
("2028-01-17", 1, "Martin Luther King Jr. Day"),
("2028-02-21", 1, "Washington's Birthday / Presidents Day"),
("2028-05-29", 1, "Memorial Day"),
("2028-07-04", 1, "Independence Day"),
("2028-09-04", 1, "Labor Day"),
("2028-10-09", 1, "Columbus Day"),
("2028-11-10", 1, "Veterans Day (Observed)"),
("2028-11-23", 1, "Thanksgiving Day"),
("2028-12-25", 1, "Christmas Day"),

("2029-01-01", 1, "New Year's Day (Observed)"),
("2029-01-15", 1, "Martin Luther King Jr. Day"),
("2029-02-19", 1, "Washington's Birthday / Presidents Day"),
("2029-05-28", 1, "Memorial Day"),
("2029-07-04", 1, "Independence Day"),
("2029-09-03", 1, "Labor Day"),
("2029-10-08", 1, "Columbus Day"),
("2029-11-12", 1, "Veterans Day (Observed)"),
("2029-11-22", 1, "Thanksgiving Day"),
("2029-12-25", 1, "Christmas Day"),

("2030-01-01", 1, "New Year's Day (Observed)"),
("2030-01-21", 1, "Martin Luther King Jr. Day"),
("2030-02-18", 1, "Washington's Birthday / Presidents Day"),
("2030-05-27", 1, "Memorial Day"),
("2030-07-04", 1, "Independence Day"),
("2030-09-02", 1, "Labor Day"),
("2030-10-14", 1, "Columbus Day"),
("2030-11-11", 1, "Veterans Day (Observed)"),
("2030-11-28", 1, "Thanksgiving Day"),
("2030-12-25", 1, "Christmas Day"),
]

df = spark.createDataFrame(rows, ["holiday_date","is_holiday","holiday_name"]) \
          .withColumn("holiday_date", F.to_date("holiday_date"))

df.write.mode("overwrite").format("delta").saveAsTable("calendar_holidays")
display(spark.table("calendar_holidays").orderBy("holiday_date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
