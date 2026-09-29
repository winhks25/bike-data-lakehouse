# SILVER LAYER - CRM Sales Details

import pyspark.sql.functions as F

# 1. Read from Bronze

df = spark.table("workspace.bronze.crm_sales_details")

# 2. Rename columns

df = (
    df
    .withColumnRenamed("sls_ord_num", "order_number")
    .withColumnRenamed("sls_prd_key", "product_key")
    .withColumnRenamed("sls_cust_id", "customer_id")
    .withColumnRenamed("sls_order_dt", "order_date")
    .withColumnRenamed("sls_ship_dt", "ship_date")
    .withColumnRenamed("sls_due_dt", "due_date")
    .withColumnRenamed("sls_sales", "sales_amount")
    .withColumnRenamed("sls_quantity", "quantity")
    .withColumnRenamed("sls_price", "unit_price")
)

# 3. Convert dates

df = (
    df
    .withColumn(
        "order_date",
        F.try_to_timestamp(
            F.col("order_date").cast("string"),
            F.lit("yyyyMMdd")
        ).cast("date")
    )
    .withColumn(
        "ship_date",
        F.try_to_timestamp(
            F.col("ship_date").cast("string"),
            F.lit("yyyyMMdd")
        ).cast("date")
    )
    .withColumn(
        "due_date",
        F.try_to_timestamp(
            F.col("due_date").cast("string"),
            F.lit("yyyyMMdd")
        ).cast("date")
    )
)


# 4. Clean unit price

df = df.withColumn(
    "unit_price",
    F.when(
        F.col("unit_price").isNull()
        & F.col("sales_amount").isNotNull()
        & (F.col("quantity") > 0),
        F.abs(F.col("sales_amount") / F.col("quantity"))
    )
    .otherwise(F.abs(F.col("unit_price")))
)


# 5. Clean sales amount

df = df.withColumn(
    "sales_amount",
    F.when(
        F.col("sales_amount").isNull()
        | (F.col("sales_amount") <= 0)
        | (
            F.col("sales_amount")
            != F.col("quantity") * F.col("unit_price")
        ),
        F.col("quantity") * F.col("unit_price")
    )
    .otherwise(F.col("sales_amount"))
)

# 6. Remove exact duplicate rows

df = df.dropDuplicates()

# 7. Foreign key validation

customer_df = (
    spark.table("workspace.silver.crm_cust_info")
    .select("customer_id")
    .distinct()
)

product_df = (
    spark.table("workspace.silver.crm_prd_info")
    .select("product_key")
    .distinct()
)

invalid_customers = df.join(
    customer_df,
    on="customer_id",
    how="left_anti"
)

invalid_products = df.join(
    product_df,
    on="product_key",
    how="left_anti"
)

# 8. Final validation

df = df.filter(
    (F.col("quantity") > 0)
    & (F.col("unit_price") > 0)
    & (F.col("sales_amount") > 0)
)

# 9. Write to Silver

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.crm_sales_details")
)