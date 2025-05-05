import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="silver_sales",
  comment="Cleaned sales data joined with product details"
)
@dlt.expect("valid_quantity", "quantity >= 0")
@dlt.expect("known_product", "product_id IS NOT NULL")
@dlt.expect("known_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer", col("customer_name").isNotNull())
def silver_sales():
    sales = dlt.read_stream("bronze_sales")
    products = dlt.read("silver_products")
    customers = dlt.read("silver_customers")

    df = (
        sales
            .dropDuplicates(["sale_id"])
            .join(products, on="product_id", how="left")
            .join(customers, on="customer_id", how="left")
            .select(
                col("sale_id"),
                col("product_id"),
                col("name").alias("product_name"),
                col("category"),
                col("price"),
                col("quantity"),
                (col("price") * col("quantity")).alias("total_amount"),
                col("customer_id"),
                col("customer_name"),
                col("loyalty_status"),
                col("sale_date")
            )
    )

    return df
