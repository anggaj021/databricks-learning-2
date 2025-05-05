import dlt
from pyspark.sql.functions import col

@dlt.table(
  name="silver_customers",
  comment="Cleaned and deduplicated customer data ready for consumption."
)

@dlt.expect_or_drop("customer_name_not_null", col("customer_name").isNotNull())

def silver_customers():
    df = (
        dlt.read_stream("bronze_customers")
        .dropDuplicates(["customer_id"])
        #.filter(col("customer_name").isNotNull())
        .select(
            col("customer_id").cast("int"),
            col("customer_name"),
            col("loyalty_status")
        )
    )

    return df
