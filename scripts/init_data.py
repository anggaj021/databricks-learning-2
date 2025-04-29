from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Paths
base_path = "/Volumes/angga/default/data/"
products_path = base_path + "products/"
sales_path = base_path + "sales/"
customers_path = base_path + "customers/"

# Create directories if not exists
os.makedirs(products_path, exist_ok=True)
os.makedirs(sales_path, exist_ok=True)
os.makedirs(customers_path, exist_ok=True)

# 1. Products Data
products = [
    Row(product_id=1, product_name="Apple", category="Fruit", price=5000),
    Row(product_id=2, product_name="Banana", category="Fruit", price=2000),
    Row(product_id=3, product_name="Carrot", category="Vegetable", price=3000),
    Row(product_id=4, product_name="Desk", category="Furniture", price=200000),
    Row(product_id=5, product_name="Chair", category="Furniture", price=100000)
]

products_df = spark.createDataFrame(products)
products_df.write.mode("overwrite").option("header", "true").csv(products_path)

# 2. Sales Data
sales = [
    Row(sale_id=1001, product_id=1, customer_id=501, quantity=5, sale_date="2024-04-01"),
    Row(sale_id=1002, product_id=2, customer_id=502, quantity=3, sale_date="2024-04-01"),
    Row(sale_id=1003, product_id=1, customer_id=503, quantity=2, sale_date="2024-04-02"),
    Row(sale_id=1004, product_id=4, customer_id=504, quantity=1, sale_date="2024-04-03"),
    Row(sale_id=1005, product_id=5, customer_id=501, quantity=1, sale_date="2024-04-04")
]

sales_df = spark.createDataFrame(sales)
sales_df.write.mode("overwrite").option("header", "true").csv(sales_path)

# 3. Customers Data
customers = [
    Row(customer_id=501, customer_name="John Doe", loyalty_status="Gold"),
    Row(customer_id=502, customer_name="Jane Smith", loyalty_status="Silver"),
    Row(customer_id=503, customer_name="Bob Brown", loyalty_status="Bronze"),
    Row(customer_id=504, customer_name="Alice White", loyalty_status="Gold")
]

customers_df = spark.createDataFrame(customers)
customers_df.write.mode("overwrite").option("header", "true").csv(customers_path)

print("Dummy Data Created Successfully")