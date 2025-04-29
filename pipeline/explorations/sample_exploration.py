# Databricks notebook source
spark.sql("select * from angga.default.bronze_products").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from angga.default.silver_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM angga.default.silver_sales WHERE customer_id = 504;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE angga.default.silver_sales ZORDER BY (customer_id, product_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM angga.default.silver_sales WHERE customer_id = 504;
