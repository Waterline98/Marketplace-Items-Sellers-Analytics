import io
import sys
import uuid
import os
import logging

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_REGION = os.getenv("S3_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

DATA_PATH = os.getenv("INPUT_ITEMS_PATH")
TARGET_PATH = os.getenv("TARGET_PATH")

if not all([S3_ENDPOINT, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DATA_PATH, TARGET_PATH]):
    raise ValueError("Не все переменные окружения загружены. Проверьте файл .env")


def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
            .config('spark.hadoop.fs.s3a.region', S3_REGION)
            .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID)
            .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)
            .getOrCreate())


def main():
    spark = _spark_session()

    df = spark.read.parquet(DATA_PATH)
    
    result_df = df.withColumn(
        "returned_items_count", 
        F.round(F.col("ordered_items_count") * (F.col("avg_percent_to_sold") / 100)).cast("int")
    ).withColumn(
        "potential_revenue",
        (F.col("availability_items_count") + F.col("ordered_items_count")) * F.col("item_price")
    ).withColumn(
        "total_revenue", 
        (F.col("ordered_items_count") - F.col("returned_items_count")) * F.col("item_price")
    ).withColumn(
        "avg_daily_sales",
        F.col("goods_sold_count") / F.col("days_on_sell")
    ).withColumn(
        "days_to_sold",
        F.when(F.col("avg_daily_sales") > 0, F.col("availability_items_count") / F.col("avg_daily_sales"))
        .otherwise(0)
    )
    
    window_spec = Window.orderBy("item_rate")
    result_df = result_df.withColumn(
        "item_rate_percent",
        F.percent_rank().over(window_spec)
    )
    
    final_columns = [
        "sku_id", "title", "category", "brand", "seller", "group_type", "country",
        "availability_items_count", "ordered_items_count", "warehouses_count", 
        "item_price", "goods_sold_count", "item_rate", "days_on_sell", 
        "avg_percent_to_sold", "returned_items_count", "potential_revenue", 
        "total_revenue", "avg_daily_sales", "days_to_sold", "item_rate_percent"
    ]
    
    result_df = result_df.select(*final_columns)
    
    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    
    print("Job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
