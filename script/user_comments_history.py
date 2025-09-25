#!/usr/bin/env python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Notes:
# Permalink
# https://www.reddit.com/r/{subreddit}/comments/{link_id_without_t3}/_/{comment_id}
# link_id:
# t3_...: the post ID (submission ID)
# parent_id:
# t3_...: parent is a post
# t1_...:parent is a comment

def main(params):
    # arguments
    input_folder  = params.input_folder
    output_folder = params.output_folder

    spark = SparkSession.builder\
                .master("local[*]") \
                .appName("user-comments") \
                .getOrCreate()

    df = spark.read.parquet(input_folder)

    df_authors = spark.read \
        .option("header", "true") \
        .csv(f"{output_folder}/users")

    df_authors = df_authors.withColumnRenamed("year", "source_year") \
                        .withColumnRenamed("subreddit_id", "source_subreddit")

    df_sampled = df.join(df_authors, \
                df.author == df_authors.sampled_authors, \
                "inner")\
                        .drop(df_authors.sampled_authors)

    df_sampled = df_sampled.withColumn("permalink", 
                                F.concat(
                                    F.lit("https://www.reddit.com/r/"),
                                    F.col("subreddit"),
                                    F.lit("/comments/"),
                                    F.regexp_replace(F.col("link_id"), "t3_", ""),
                                    F.lit("/_/"),
                                    F.col("id")
                                )
                            )
    
    # save dataframes:
    df_comments_history_full = df_sampled.select("author",
                                                "subreddit_id",
                                                "subreddit",
                                                "year",
                                                "month",
                                                "body",
                                                "created_utc_ts",
                                                "link_id",
                                                "permalink",
                                                "source_subreddit",
                                                "source_year")

    df_comments_history_full \
                .coalesce(1) \
                .write \
                .option("header", True) \
                .mode("overwrite") \
                .parquet(f"{output_folder}/comments_history/")
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Parameters for full comments history for sampled users")
    parser.add_argument("--input_folder",
                         help="Path to the folder containing input Reddit data files")
    parser.add_argument("--output_folder",
                         help="Path to the folder where data sampled are saved")
    
    args = parser.parse_args()
    main(args)