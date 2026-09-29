from pyspark.sql import SparkSession


# Reuse the session supplied by Databricks Runtime. When this file is launched
# locally, create the remote Databricks Connect session.
spark = SparkSession.getActiveSession()
if spark is None:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()

query = """
SELECT
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.sales_amount,
    sd.quantity,
    sd.unit_price AS price
FROM workspace.silver.crm_sales AS sd
LEFT JOIN workspace.gold.dim_products AS pr
    ON sd.product_key = pr.product_number
LEFT JOIN workspace.gold.dim_customers AS cu
    ON sd.customer_id = cu.customer_id
"""

df = spark.sql(query)

# Write to gold layer
(
    df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.fact_sales")
)

df.show(10, truncate=False)
