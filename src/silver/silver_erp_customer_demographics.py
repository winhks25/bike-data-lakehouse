# SILVER LAYER - ERP Customer AZ12

import pyspark.sql.functions as F

# 1. Read from Bronze

df = spark.table("workspace.bronze.erp_cust_az12")

# 2. Rename columns

df = (
    df
    .withColumnRenamed("CID", "customer_id")
    .withColumnRenamed("BDATE", "birth_date")
    .withColumnRenamed("GEN", "gender")
)

# 3. Clean customer ID

df = df.withColumn(
    "customer_id",
    F.regexp_replace(F.col("customer_id"), "^(NASAW|AW)", "").cast("int")
)

# 4. Clean birth date

df = df.withColumn(
    "birth_date",
    F.to_date(F.col("birth_date"), "yyyy-MM-dd")
)

df = df.withColumn(
    "birth_date",
    F.when(
        F.col("birth_date") > F.current_date(),
        F.lit(None).cast("date")
    ).otherwise(F.col("birth_date"))
)

# 5. Clean gender

df = df.withColumn(
    "gender",
    F.when(
        F.upper(F.trim(F.col("gender"))).isin("M", "MALE"),
        "Male"
    )
    .when(
        F.upper(F.trim(F.col("gender"))).isin("F", "FEMALE"),
        "Female"
    )
    .otherwise(F.lit(None).cast("string"))
)

# 6. Remove exact duplicate rows

df = df.dropDuplicates()

# 7. Foreign key validation

customer_df = (
    spark.table("workspace.silver.crm_customer")
    .select("customer_id")
    .distinct()
)

invalid_customers = df.join(
    customer_df,
    on="customer_id",
    how="left_anti"
)

# 8. Write to Silver
table_name = "workspace.silver.erp_customer_demographics"
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

# 9. Write invalid references to Bronze for review
table_name = "workspace.bronze.erp_cust_az12_invalid"
(
    invalid_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)
