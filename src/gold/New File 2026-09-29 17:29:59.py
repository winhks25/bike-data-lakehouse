for name in [
    "workspace.silver.crm_cust_info",
    "workspace.silver.erp_cust_az12",
    "workspace.silver.erp_loc_a101",
]:
    print(f"\nTABLE: {name}")
    table = spark.table(name)
    table.printSchema()

    id_columns = [
        c for c in ["customer_id", "customer_key"]
        if c in table.columns
    ]
    table.select(*id_columns).show(5, truncate=False)