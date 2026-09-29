"""Build the current-product dimension in the Gold layer."""

from pyspark.sql import SparkSession


# Reuse the session supplied by Databricks Runtime. When this file is launched
# from the VS Code extension, create the remote Databricks Connect session.
spark = SparkSession.getActiveSession()
if spark is None:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()

query = """
SELECT
    ROW_NUMBER() OVER (
        ORDER BY pn.product_start_date, pn.product_key, pn.product_id
    ) AS product_key,
    pn.product_id,
    pn.product_key AS product_number,
    pn.product_name,
    REPLACE(SUBSTRING(pn.product_key, 1, 5), '-', '_') AS category_id,
    pc.category,
    pc.subcategory,
    pc.maintenance AS maintenance_flag,
    pn.product_cost,
    pn.product_line,
    pn.product_start_date AS start_date,
    pn.product_color,
    pn.product_size,
    pn.product_capacity_oz
FROM workspace.silver.crm_product AS pn
LEFT JOIN workspace.silver.erp_product_category AS pc
    ON REPLACE(SUBSTRING(pn.product_key, 1, 5), '-', '_') = pc.category_id
WHERE pn.product_end_date IS NULL
"""

df = spark.sql(query)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.dim_products")
)

df.show(10, truncate=False)
