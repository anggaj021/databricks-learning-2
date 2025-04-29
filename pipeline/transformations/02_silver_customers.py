import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="silver_customers",
  comment="Cleaned and deduplicated customer data ready for consumption."
)

def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_name").isNotNull())
        .select(
            col("customer_id").cast("int"),
            col("customer_name"),
            col("loyalty_status")
        )
    )
