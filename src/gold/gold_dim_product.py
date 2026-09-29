query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.start_date, pn.product_number) AS product_key, -- Surrogate key
    pn.product_id,
    pn.product_number,
    pn.product_name,
    pn.category_id,
    pc.category,
    pc.subcategory,
    pc.maintenance_flag,
    pn.product_line,
    pn.start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_product_category pc
    ON pn.category_id = pc.category_id
--WHERE pn.end_date IS NULL; -- Filter out all historical data
"""
df = spark.sql(query)

df.limit(10).display()