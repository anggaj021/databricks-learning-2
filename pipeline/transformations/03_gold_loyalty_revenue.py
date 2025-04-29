import dlt
from pyspark.sql.functions import col, sum, to_date

@dlt.table(
  name="gold_loyalty_revenue",
  comment="Total revenue and quantity sold grouped by customer loyalty status"
)
def gold_loyalty_revenue():
    sales = dlt.read("silver_sales")
    
    return (
        sales.groupBy("loyalty_status")
            .agg(
               sum("total_amount").alias("total_revenue"),
               sum("quantity").alias("total_quantity")
            )
            .orderBy("total_revenue", ascending=False)
    )
