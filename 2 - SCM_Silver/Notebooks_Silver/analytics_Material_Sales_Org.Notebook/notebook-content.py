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
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "98f863b2-458a-4924-924e-617909254b9a",
# META       "known_warehouses": [
# META         {
# META           "id": "98f863b2-458a-4924-924e-617909254b9a",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, when, substring, expr, to_date, length,regexp_replace,lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mvke_df = spark.read.table("sap.MVKE").filter("MANDT = '100'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step1_df = mvke_df.select(
    col("MANDT").alias("Client")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step1_df = mvke_df.select(
    col("MANDT").alias("Client")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step2_df = mvke_df.select(
    col("MANDT").alias("Client"),
    regexp_replace(col("MATNR"), "^0+", "").alias("Material")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step3_df = mvke_df.select(
    col("MANDT").alias("Client"),
    regexp_replace(col("MATNR"), "^0+", "").alias("Material"),
    col("VKORG").alias("Sales Org."),
    col("VTWEG").alias("Distr. Channel")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step4_df = mvke_df.select(
    col("MANDT").alias("Client"),
    regexp_replace(col("MATNR"), "^0+", "").alias("Material"),
    col("VKORG").alias("Sales Org."),
    col("VTWEG").alias("Distr. Channel"),
    col("LVORM").alias("DF Chain level"),
    col("VERSG").alias("Matl stats grp"),
    col("BONUS").alias("Vol. rebate grp"),
    col("PROVG").alias("Commission grp"),
    col("SKTOF").alias("Cash discount"),
    col("VMSTA").alias("Prod Life Cycle")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step5_df = mvke_df.select(
    col("MANDT").alias("Client"),
    regexp_replace(col("MATNR"), "^0+", "").alias("Material"),
    col("VKORG").alias("Sales Org."),
    col("VTWEG").alias("Distr. Channel"),
    col("LVORM").alias("DF Chain level"),
    col("VERSG").alias("Matl stats grp"),
    col("BONUS").alias("Vol. rebate grp"),
    col("PROVG").alias("Commission grp"),
    col("SKTOF").alias("Cash discount"),
    col("VMSTA").alias("Prod Life Cycle"),

    when(col("VMSTD") == 0, "1900-01-01")
    .otherwise(to_date(col("VMSTD").cast("string"), "yyyyMMdd"))
    .alias("Valid from")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step6_df = step5_df.join(
    mvke_df.select(
        col("AUMNG").alias("Min.order qty"),
        col("LFMNG").alias("Min. dely qty"),
        col("EFMNG").alias("MakeToOrder qty"),
        col("SCMNG").alias("Delivery unit"),
        col("SCHME").alias("Unit of measure"),
        col("VRKME").alias("Sales unit")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step7_df = mvke_df.select(
    col("PRODH").alias("Prod.hierarchy"),
    col("DWERK").alias("Deliver.Plant"),
    col("MTPOS").alias("Item cat.group")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step8_df = mvke_df.select(
    col("KONDM").alias("Mat.pricing grp"),
    col("KTGRM").alias("Acct asgnmt grp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

step9_df = mvke_df.select(
    col("MVGR1").alias("MaterialGroup 1"),
    col("MVGR2").alias("MaterialGroup 2"),
    col("MVGR3").alias("BU Lead Time"),
    col("MVGR4").alias("MaterialGroup 4"),
    col("MVGR5").alias("MaterialGroup 5")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bev_df = mvke_df.select(
    col("`/BEV1/EMLGRP`").alias("Emp. Group"),
    col("`/BEV1/EMDRCKSPL`").alias("Print Column"),
    col("`/BEV1/RPBEZME`").alias("BUoM"),
    col("`/BEV1/RPSNS`").alias("None")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = mvke_df.select(
    col("MANDT").alias("client"),
    regexp_replace(col("MATNR"), "^0+", "").alias("Material"),
    col("VKORG").alias("sales_org"),
    col("VTWEG").alias("distr_channel"),

    col("LVORM").alias("df_chain_level"),
    col("VERSG").alias("matl_stats_grp"),
    col("BONUS").alias("vol_rebate_grp"),
    col("PROVG").alias("commission_grp"),
    col("SKTOF").alias("cash_discount"),
    col("VMSTA").alias("prod_life_cycle"),

    when(col("VMSTD") == 0, "1900-01-01")
    .otherwise(to_date(col("VMSTD").cast("string"), "yyyyMMdd"))
    .alias("valid_from"),

    col("AUMNG").alias("min_order_qty"),
    col("LFMNG").alias("min_dely_qty"),
    col("EFMNG").alias("make_to_order_qty"),
    col("SCMNG").alias("delivery_unit"),
    col("SCHME").alias("unit_of_measure"),
    col("VRKME").alias("sales_unit"),

    col("PRODH").alias("prod_hierarchy"),
    col("DWERK").alias("deliver_plant"),
    col("MTPOS").alias("item_cat_group"),

    col("KONDM").alias("mat_pricing_grp"),
    col("KTGRM").alias("acct_asgnmt_grp"),

    col("MVGR1").alias("material_group_1"),
    col("MVGR2").alias("material_group_2"),
    col("MVGR3").alias("bu_lead_time"),
    col("MVGR4").alias("material_group_4"),
    col("MVGR5").alias("material_group_5"),

    col("`/BEV1/EMLGRP`").alias("emp_group"),
    col("`/BEV1/EMDRCKSPL`").alias("print_column"),
    col("`/BEV1/RPBEZME`").alias("buom"),
    col("`/BEV1/RPSNS`").alias("none")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df.write.mode("overwrite").saveAsTable("SCM_Silver_LH.analytics.material_sales_organization") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS analytics.material_sales_org")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
