"""Build the current-product dimension in the Gold layer."""

from pyspark.sql import SparkSession


# Reuse the session supplied by Databricks Runtime. When this file is launched
# from the VS Code extension, create the remote Databricks Connect session.
spark = SparkSession.getActiveSession()
if spark is None:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()

query = """
WITH ranked_products AS (
    SELECT
        pn.*,
        ROW_NUMBER() OVER (
            PARTITION BY SUBSTRING(pn.product_key, 7)
            ORDER BY pn.product_start_date DESC, pn.product_id DESC
        ) AS product_record_rank
    FROM workspace.silver.crm_product AS pn
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY pn.product_start_date, pn.product_key, pn.product_id
    ) AS product_key,
    pn.product_id,
    SUBSTRING(pn.product_key, 7) AS product_number,
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
FROM ranked_products AS pn
LEFT JOIN workspace.silver.erp_product_category AS pc
    ON REPLACE(SUBSTRING(pn.product_key, 1, 5), '-', '_') = pc.category_id
WHERE pn.product_record_rank = 1
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
