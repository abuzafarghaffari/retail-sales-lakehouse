from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.table("dev.retail_sales.bronze_sales")

silver_df = (
    bronze_df
    .withColumn("order_id",F.expr("try_cast(order_id AS INT)"))
    .withColumn("order_date",F.to_date("order_date","yyyy-MM-dd"))
    .withColumn("quantity",F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price",F.expr("try_cast(unit_price AS DOUBLE)"))
)

# valid_df  = silver_df.filter(
#     F.col("order_id").isNotNull()
#     & F.col("oder_date").isNotNull()
#     & F.col("customer_id").isNotNull()
#     & (F.trim(F.col("customer_id")) !="")
#     & F.col("product_id").isNotNull()
#     & (F.trim(F.col("product_id")) != "")
#     & (F.col("quantity") > 0)
#     & (F.col("unit_price") >= 0)
# )

# invalid_df = silver_df.subtract(valid_df)

validation_condition  = (
    F.col("order_id").isNotNull()
    & F.col("order_date").isNotNull()
    & F.col("customer_id").isNotNull()
    & (F.trim(F.col("customer_id")) !="")
    & F.col("product_id").isNotNull()
    & (F.trim(F.col("product_id")) != "")
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
)

valid_df  = silver_df.filter(validation_condition)

valid_df = valid_df.withColumn("total_amount",
       F.col("quantity") * F.col("unit_price")                        
                               )

silver_clean_df = valid_df.dropDuplicates(["order_id"])


invalid_df = silver_df.filter(
    ~F.coalesce(validation_condition, F.lit(False))
)

invalid_df = invalid_df.withColumn(
    "rection_reason",
    F.when(F.col("quantity").isNull(),"Invalid quantity")
     .when(F.col("quantity") <= 0,"Quantity must be greater than zero")
     .when(F.col("unit_price").isNull(),"Invalid unit price")
     .when(F.col("unit_price") <=0,"Negative unit price")
     .otherwise("Missing or invalid required filed")
)

(
    silver_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable("dev.retail_sales.silver_sales")
)

(
    invalid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable("dev.retail_sales.quarantine_sales")
)