import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="bronze_customers",
  comment="Raw ingested customer data with constraints."
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/angga/default/data/customers/")
            .withColumn("customer_id", col("customer_id").cast("int"))
            .withColumn("customer_name", col("customer_name").cast("string"))
            .withColumn("loyalty_status", col("loyalty_status").cast("string"))
    )