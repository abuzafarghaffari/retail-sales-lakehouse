from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

s3_path = "s3://zafar-s3-databrick-demo-bucket/project01/incoming/"
checkpoint_path = "/Volumes/dev/retail_sales/pipeline_files/checkpoint/"

bronze_df =(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("header","true")
    .option("cloudFiles.schemaLocation","/Volumes/dev/retail_sales/pipeline_files/schema/")
    .load(s3_path)
)

query =(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation",checkpoint_path)
    .trigger(availableNow=True)
    .toTable("dev.retail_sales.bronze_sales")
)