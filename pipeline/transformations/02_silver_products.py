import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="silver_products",
  comment="Cleaned and deduplicated product data ready for consumption."
)
@dlt.expect("price_positive", "price >= 0")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .dropDuplicates(["product_id"])
        .filter(col("product_name").isNotNull())
        .select(
            col("product_id").cast("int"),
            col("product_name").alias("name"),
            col("category"),
            col("price").cast("double")
        )
    )
