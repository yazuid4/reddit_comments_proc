import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, LongType
)

DATA_SCHEMA = StructType([
    StructField("controversiality", IntegerType(), True),
    StructField("body", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("stickied", BooleanType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ups", IntegerType(), True),
    StructField("author_flair_css_class", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("author_flair_text", StringType(), True),
    StructField("author", StringType(), True),
    StructField("id", StringType(), True),
    StructField("edited", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("gilded", IntegerType(), True),
    StructField("distinguished", StringType(), True),
    StructField("retrieved_on", LongType(), True),
])


def transform(args):
    bucket_name = args.bucket_name
    input_file  = args.input_file
    output_dir  = args.output_dir
    
    # spark_setup:    
    spark = SparkSession.builder.appName("spark-etl-parquet").getOrCreate() 
    
    # read file
    df = spark.read.schema(DATA_SCHEMA)\
                    .json(f"gs://{bucket_name}/decompressed/{input_file}")

    # edited type
    df = df.withColumn(
    "edited",
    F.when(F.col("edited").cast("boolean").isNotNull(), F.col("edited").cast("boolean"))
     .otherwise(F.lit(False))
    )

    #  timestamps
    df = df.withColumn("created_utc_ts", 
                    from_unixtime(col("created_utc")).cast("timestamp"))\
           .withColumn("retrieved_on_ts", 
                    from_unixtime(col("retrieved_on")).cast("timestamp"))\
           .drop("retrieved_on") \
           .drop("created_utc")
    
    # year & month
    year  = input_file.split('-')[0].split('_')[1]
    month = input_file.split('-')[1].split('.')[0]
    df = df.withColumn("month", F.lit(month)) \
                        .withColumn("year", F.lit(year))
    # save
    df.repartition(4).write\
        .parquet(f"gs://{bucket_name}/{output_dir}/{year}/{month}",
                    mode="overwrite")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="")

    parser.add_argument("--bucket_name", required=True, help="")
    parser.add_argument("--input_file", required=True, help="")
    parser.add_argument("--output_dir", required=True, help="")

    arguments = parser.parse_args()
    transform(arguments)