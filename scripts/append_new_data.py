from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Path
sales_path = "/Volumes/angga/default/data/sales/"

# Simulated new data (e.g. new file arrives from upstream system)
new_sales = [
    Row(sale_id=1007, product_id=3, customer_id=502, quantity=6, sale_date="2024-04-06"),
    Row(sale_id=1008, product_id=2, customer_id=504, quantity=4, sale_date="2024-04-05")
]

new_sales_df = spark.createDataFrame(new_sales)

# Use timestamp to create a new file (simulate streaming arrival)
new_file_path = os.path.join(sales_path, f"new_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
new_sales_df.coalesce(1).write.mode("append").option("header", "true").csv(new_file_path)

print("New Sales Data Appended")
