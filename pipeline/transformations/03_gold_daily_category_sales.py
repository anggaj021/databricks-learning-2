import dlt
from pyspark.sql.functions import col, sum, to_date

@dlt.table(
  name="gold_daily_category_sales",
  comment="Daily revenue and quantity sold by product category"
)
def gold_daily_category_sales():
    sales = dlt.read("silver_sales")

    return (
        sales
            .withColumn("sale_date", to_date("sale_date"))
            .groupBy("sale_date", "category")
            .agg(
                sum("quantity").alias("total_quantity"),
                sum("total_amount").alias("total_revenue")
            )
            .orderBy("sale_date", "category")
    )
