import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="bronze_sales",
  comment="Raw sales data from CSV files"
)

def bronze_sales():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load("/Volumes/angga/default/data/sales/")
            .withColumn("sale_id", col("sale_id").cast("int"))
            .withColumn("product_id", col("product_id").cast("int"))
            .withColumn("customer_id", col("customer_id").cast("int"))
            .withColumn("quantity", col("quantity").cast("int"))
            .withColumn("sale_date", col("sale_date").cast("date"))
    )
