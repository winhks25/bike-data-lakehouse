# SILVER LAYER - ERP Product Category G1V2

import pyspark.sql.functions as F

# 1. Read from Bronze

df = spark.table("workspace.bronze.erp_px_cat_g1v2")

# 2. Rename columns

df = (
    df
    .withColumnRenamed("ID", "category_id")
    .withColumnRenamed("CAT", "category")
    .withColumnRenamed("SUBCAT", "subcategory")
    .withColumnRenamed("MAINTENANCE", "maintenance")
)

# 3. Clean string columns

df = (
    df
    .withColumn("category_id", F.trim(F.col("category_id")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("subcategory", F.trim(F.col("subcategory")))
)

# 4. Clean maintenance

df = df.withColumn(
    "maintenance",
    F.when(
        F.upper(F.trim(F.col("maintenance"))) == "YES",
        F.lit(True)
    )
    .when(
        F.upper(F.trim(F.col("maintenance"))) == "NO",
        F.lit(False)
    )
    .otherwise(F.lit(None).cast("boolean"))
)

# 5. Remove exact duplicate rows

df = df.dropDuplicates()

# 6. Write to Silver
table_name = "workspace.silver.erp_product_category"
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)