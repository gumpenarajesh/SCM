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

from pyspark.sql.functions import col, when, substring, length, instr, expr, regexp_replace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import col, substring, expr

# # 🔹 Load T179T (language filter important)
# t179t= spark.table("SCM_Bronze_LH.sap.t179t").filter("MANDT='100'").filter("SPRAS ='E'")

# # 🔹 Base Data (same as your inner query)
# base_df = t179t.select(
#     col("PRODH").alias("Product_Hierarchy"),

#     # PH Level 1 (numeric SAP logic)
#     substring("PRODH", 1, 5).alias("PH_Level_1"),

#     # PH Level 2
#     substring("PRODH", 1, 10).alias("PH_Level_2"),

#     # PH Level 3 (full hierarchy)
#     col("PRODH").alias("PH_Level_3")
# )

# # 🔹 Aliases for joins (same table used multiple times)
# t1 = t179t.select(
#     col("PRODH").alias("L1"),
#     col("VTEXT").alias("PH_Level_1_Text")
# )

# t2 = t179t.select(
#     col("PRODH").alias("L2"),
#     col("VTEXT").alias("PH_Level_2_Text")
# )

# t3 = t179t.select(
#     col("PRODH").alias("L3"),
#     col("VTEXT").alias("PH_Level_3_Text")
# )

# # 🔹 Joins (replacing all subqueries + T25A7)
# df = base_df \
#     .join(t1, base_df.PH_Level_1 == t1.L1, "left") \
#     .join(t2, base_df.PH_Level_2 == t2.L2, "left") \
#     .join(t3, base_df.PH_Level_3 == t3.L3, "left")

# # 🔹 Final Output (same as your view columns)
# final_df = df.select(
#     col("Product_Hierarchy"),

#     # T25A7.WWLNE replacement
#     col("PH_Level_1").alias("PH Product Line"),

#     # T25A7.BEZEK replacement
#     col("PH_Level_1_Text").alias("PH Product Line Text"),

#     col("PH_Level_1").alias("PH Level 1"),
#     col("PH_Level_1_Text").alias("PH Level 1 Text"),

#     col("PH_Level_2").alias("PH Level 2"),
#     col("PH_Level_2_Text").alias("PH Level 2 Text"),

#     col("PH_Level_3").alias("PH Level 3"),
#     col("PH_Level_3_Text").alias("PH Level 3 Text")
# )

# # 🔹 Show result
# final_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import col, substring, concat, lit, length, when

# # 1. Load Data from Bronze
# t179t = spark.table("SCM_Bronze_LH.sap.t179t").filter("MANDT='100'").filter("SPRAS ='E'")

# # 2. Base Transformation with Dynamic Dots and Product Line
# base_df = t179t.select(
#     col("PRODH").alias("RAW_PRODH"),
#     col("VTEXT").alias("RAW_VTEXT"),
    
#     # PH Product Line: Top-most level (First 5 chars) bina dot ke
#     substring("PRODH", 1, 5).alias("PH_Product_Line"),

#     # Product_Hierarchy Dynamic Logic (A1000..10001...000001)
#     when(length(col("PRODH")) == 5, 
#          concat(substring("PRODH", 1, 5), lit("..")))
#     .when(length(col("PRODH")) == 10, 
#          concat(substring("PRODH", 1, 5), lit(".."), substring("PRODH", 6, 5), lit("...")))
#     .when(length(col("PRODH")) == 16, 
#          concat(substring("PRODH", 1, 5), lit(".."), substring("PRODH", 6, 5), lit("..."), substring("PRODH", 11, 6)))
#     .otherwise(col("PRODH")).alias("Product_Hierarchy")
# )

# # 3. Filtering: Tools (00001) aur Drinks (00002) ko hatana
# df_filtered = base_df.filter(
#     (~col("RAW_PRODH").startswith("00001")) & 
#     (~col("RAW_PRODH").startswith("00002"))
# )

# # 4. Text Joins (Descriptions map karne ke liye)
# # L1_text humein PH Product Line Text dega
# t1_text = t179t.select(col("PRODH").alias("L1_ID"), col("VTEXT").alias("PH_Product_Line_Text"))
# t2_text = t179t.select(col("PRODH").alias("L2_ID"), col("VTEXT").alias("PH_Level_2_Text"))

# final_df = df_filtered \
#     .join(t1_text, df_filtered.PH_Product_Line == t1_text.L1_ID, "left") \
#     .join(t2_text, substring(df_filtered.RAW_PRODH, 1, 10) == t2_text.L2_ID, "left")

# # 5. Final Column Formatting (Third Column: PH Product Line Text)
# output_df = final_df.select(
#     col("Product_Hierarchy"),
#     col("PH_Product_Line"),
#     col("PH_Product_Line_Text"), # Third Column - Isme Car Name aayega
    
#     # PH Level 1 (Code + ..)
#     concat(substring("RAW_PRODH", 1, 5), lit("..")).alias("PH_Level_1"),
#     col("PH_Product_Line_Text").alias("PH_Level_1_Text"), # Level 1 Text same as Product Line Text
    
#     # PH Level 2 (Code1 + .. + Code2 + ...)
#     concat(substring("RAW_PRODH", 1, 5), lit(".."), substring("RAW_PRODH", 6, 5), lit("...")).alias("PH_Level_2"),
#     col("PH_Level_2_Text"),
    
#     # PH Level 3 (Full Dotted Path)
#     col("Product_Hierarchy").alias("PH_Level_3"),
#     col("RAW_VTEXT").alias("PH_Level_3_Text")
# )

# # 6. Display Result
# display(output_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, substring, concat, lit, length, when

# 1. Base Data & Transformation
# (Wahi logic jo humne finalize kiya tha)
t179t = spark.table("SCM_Bronze_LH.sap.t179t").filter("MANDT='100'").filter("SPRAS ='E'")

base_df = t179t.select(
    col("PRODH").alias("RAW_PRODH"),
    col("VTEXT").alias("RAW_VTEXT"),
    substring("PRODH", 1, 5).alias("PH_Product_Line"),
    when(length(col("PRODH")) == 5, concat(substring("PRODH", 1, 5), lit("..")))
    .when(length(col("PRODH")) == 10, concat(substring("PRODH", 1, 5), lit(".."), substring("PRODH", 6, 5), lit("...")))
    .when(length(col("PRODH")) == 16, concat(substring("PRODH", 1, 5), lit(".."), substring("PRODH", 6, 5), lit("..."), substring("PRODH", 11, 6)))
    .otherwise(col("PRODH")).alias("Product_Hierarchy")
)

# 2. Filtering
df_filtered = base_df.filter((~col("RAW_PRODH").startswith("00001")) & (~col("RAW_PRODH").startswith("00002")))

# 3. Joins for Text
t1_text = t179t.select(col("PRODH").alias("L1_ID"), col("VTEXT").alias("PH_Product_Line_Text"))
t2_text = t179t.select(col("PRODH").alias("L2_ID"), col("VTEXT").alias("PH_Level_2_Text"))

# 4. Final Selection (Strict Order)
# Yahan hum columns ko ek list mein define kar rahe hain
final_df = df_filtered \
    .join(t1_text, df_filtered.PH_Product_Line == t1_text.L1_ID, "left") \
    .join(t2_text, substring(df_filtered.RAW_PRODH, 1, 10) == t2_text.L2_ID, "left") \
    .select(
        "Product_Hierarchy",
        "PH_Product_Line",
        "PH_Product_Line_Text",
        concat(substring("RAW_PRODH", 1, 5), lit("..")).alias("PH_Level_1"),
        col("PH_Product_Line_Text").alias("PH_Level_1_Text"),
        concat(substring("RAW_PRODH", 1, 5), lit(".."), substring("RAW_PRODH", 6, 5), lit("...")).alias("PH_Level_2"),
        "PH_Level_2_Text",
        # col("Product_Hierarchy").alias("PH_Level_3"),
        # col("RAW_VTEXT").alias("PH_Level_3_Text")
    )

    



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


final_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("SCM_Silver_LH.analytics.product_hierarchy")

# Verify the order immediately
spark.table("SCM_Silver_LH.analytics.product_hierarchy").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the first 3 columns are exactly what you want
check_df = spark.table("SCM_Silver_LH.analytics.product_hierarchy")
print("Actual Physical Order in Table:")
print(check_df.columns) 

# display output to see visual alignment
display(check_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.refreshTable("SCM_Silver_LH.analytics.product_hierarchy")

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
