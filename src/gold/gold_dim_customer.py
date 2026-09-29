# GOLD LAYER - Customer Dimension

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Silver tables
crm = spark.table("workspace.silver.crm_cust_info")
erp = spark.table("workspace.silver.erp_cust_az12")
loc = spark.table("workspace.silver.erp_loc_a101")

# Inspect rows that cannot be included in the customer dimension
invalid_customers = crm.filter(F.col("customer_id").isNull())
display(invalid_customers)

# Only include customers with a valid ID
crm = crm.filter(F.col("customer_id").isNotNull())

# 2. Join customer data
df = (
    crm.alias("crm")
    .join(
        erp.alias("erp"),
        F.col("crm.customer_id") == F.col("erp.customer_id"),
        "left",
    )
    .join(
        loc.alias("loc"),
        F.col("crm.customer_id") == F.col("loc.customer_id"),
        "left",
    )
)

# 3. Build customer dimension
df = df.select(
    F.col("crm.customer_id").alias("customer_id"),
    F.col("crm.customer_key").alias("customer_number"),
    F.col("crm.customer_firstname").alias("first_name"),
    F.col("crm.customer_lastname").alias("last_name"),
    F.col("loc.country").alias("country"),
    F.col("crm.customer_marital_status").alias("marital_status"),
    F.when(
        F.col("crm.customer_gender").isNotNull()
        & (F.lower(F.col("crm.customer_gender")) != "n/a"),
        F.col("crm.customer_gender"),
    )
    .otherwise(
        F.coalesce(
            F.col("erp.gender"),
            F.lit("n/a"),
        )
    )
    .alias("gender"),
    F.col("erp.birth_date").alias("birth_date"),
    F.col("crm.customer_create_date").alias("created_date"),
)

# 4. Generate Gold surrogate key
window = Window.orderBy("customer_id")

df = (
    df.withColumn(
        "customer_key",
        F.row_number().over(window),
    )
    .select(
        "customer_key",
        "customer_id",
        "customer_number",
        "first_name",
        "last_name",
        "country",
        "marital_status",
        "gender",
        "birth_date",
        "created_date",
    )
)

# 5. Data quality checks
has_null_ids = (
    df.filter(F.col("customer_id").isNull())
    .limit(1)
    .count()
)

if has_null_ids > 0:
    raise ValueError("dim_customers contains null customer_id values.")

has_duplicate_ids = (
    df.groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if has_duplicate_ids > 0:
    raise ValueError("dim_customers contains duplicate customer_id values.")

# 6. Write Gold table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_customers")
)

# 7. Preview
display(
    spark.table("workspace.gold.dim_customers")
    .orderBy("customer_key")
    .limit(10)
)