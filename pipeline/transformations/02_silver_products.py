import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="silver_products",
  comment="Cleaned and deduplicated product data ready for consumption."
)
@dlt.expect("price_positive", "price >= 0")
#@dlt.expect_or_drop("product_name_not_null", col("name").isNotNull())
def silver_products():
    df = (
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

    return df