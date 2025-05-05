from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def format_rupiah(amount):
    if amount is None:
        return None
    try:
        # Convert to integer and format with comma as thousands separator
        formatted = f"Rp{int(amount):,}".replace(",", ".")
        return formatted
    except Exception:
        return None