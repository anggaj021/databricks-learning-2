import dlt
from pyspark.sql.functions import col, sum, to_date
from utilities import utils

@dlt.table(
    name="gold_product_sales",
    comment="Total revenue and quantity sold by product"
)
def gold_product_sales():
    sales = dlt.read("silver_sales")

    return (
        sales.groupBy("product_id", "product_name", "category")
            .agg(
                sum("quantity").alias("total_quantity"),
                sum("total_amount").alias("total_revenue")
            )
            .withColumn("formatted_revenue", utils.format_rupiah(col("total_revenue")))
            .orderBy(col("total_revenue").desc())
    )
