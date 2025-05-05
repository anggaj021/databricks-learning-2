spark.sql("USE CATALOG angga")
spark.sql("USE SCHEMA default")

tables_to_drop = [
    "bronze_sales",
    "bronze_products",
    "bronze_customers",
    "silver_sales",
    "silver_products",
    "silver_customers",
    "gold_loyalty_revenue",
    "gold_product_sales",
    "gold_daily_category_sales"
]

for table in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

file_path = "/Volumes/angga/default/data/"
dbutils.fs.rm(file_path, True)