# SILVER LAYER - ERP Location A101

import pyspark.sql.functions as F

# 1. Read from Bronze

df = spark.table("workspace.bronze.erp_loc_a101")

# 2. Rename columns

df = (
    df
    .withColumnRenamed("CID", "customer_id")
    .withColumnRenamed("CNTRY", "country")
)

# 3. Clean customer ID

df = df.withColumn(
    "customer_id",
    F.regexp_replace(
        F.col("customer_id"),
        "^AW-0*",
        ""
    ).cast("int")
)

# 4. Clean country

df = df.withColumn(
    "country",
    F.when(
        F.trim(F.col("country")) == "",
        F.lit(None).cast("string")
    )
    .when(
        F.upper(F.trim(F.col("country"))).isin("US", "USA"),
        "United States"
    )
    .when(
        F.upper(F.trim(F.col("country"))) == "DE",
        "Germany"
    )
    .otherwise(F.trim(F.col("country")))
)

# 5. Remove exact duplicate rows

df = df.dropDuplicates()

# 6. Foreign key validation

customer_df = (
    spark.table("workspace.silver.crm_cust_info")
    .select("customer_id")
    .distinct()
)

invalid_customers = df.join(
    customer_df,
    on="customer_id",
    how="left_anti"
)

print("Invalid customer references:", invalid_customers.count())

# 7. Write to Silver
table_name = "workspace.silver.erp_customer_location"
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)