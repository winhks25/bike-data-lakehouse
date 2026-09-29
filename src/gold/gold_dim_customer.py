# GOLD LAYER - Customer Dimension

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
if spark is None:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()

# 1. Read Silver tables
crm = spark.table("workspace.silver.crm_customer")
erp = spark.table("workspace.silver.erp_customer_demographics")
loc = spark.table("workspace.silver.erp_customer_location")

# Inspect rows that cannot be included in the customer dimension
invalid_customers = crm.filter(F.col("customer_id").isNull())
invalid_customers.show(10, truncate=False)

# Only include customers with a valid ID
crm = crm.filter(F.col("customer_id").isNotNull())

# Keep the most recently created CRM record when an ID occurs more than once.
latest_customer = Window.partitionBy("customer_id").orderBy(
    F.col("customer_create_date").desc_nulls_last(),
    F.col("customer_key").desc_nulls_last(),
)
crm = (
    crm.withColumn("_customer_row", F.row_number().over(latest_customer))
    .filter(F.col("_customer_row") == 1)
    .drop("_customer_row")
)

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
