import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="bronze_products",
  comment="Raw ingested product data with constraints."
)
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("valid_price", "price >= 0")
def bronze_products():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/angga/default/data/products/")
            .withColumn("product_id", col("product_id").cast("int"))
            .withColumn("price", col("price").cast("double"))
    )