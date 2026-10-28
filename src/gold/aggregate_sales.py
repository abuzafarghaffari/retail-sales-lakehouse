from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

silver_df = spark.table("dev.retail_sales.silver_sales")

# sales table
daily_sales_df = silver_df.groupBy("order_date").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("quantity").alias("total_quantity"),
    F.sum("total_amount").alias("total_revenue"),
)

(
    daily_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.retail_sales.gold_daily_sales")
)
# product table
product_sales_df = silver_df.groupBy("product_id", "product_name").agg(
    F.count("order_id").alias("total_order"),
    F.sum("quantity").alias("total_quantity"),
    F.sum("total_amount").alias("total_revenue"),
)

(
    product_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.retail_sales.gold_product_sales")
)

# customer_sales  table

customer_sales_df = silver_df.groupBy("customer_id").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("quantity").alias("total_quantity"),
    F.sum("total_amount").alias("total_spent"),
)

(
    customer_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable("dev.retail_sales.gold_customer_sales")
)