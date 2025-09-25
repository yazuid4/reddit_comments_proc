#!/usr/bin/env python
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



def run(parameters):
    # arguments
    start_year = int(parameters.start_year)
    end_year   = int(parameters.end_year)
    input_folder  = parameters.input_folder
    output_folder = parameters.output_folder

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)


    spark = SparkSession.builder.appName("user-sampling") \
                .master("local[*]").getOrCreate()
    
    # read
    df = spark.read.parquet(input_folder)

    # user sampling
    df_users_grouped = df.select("year", "subreddit_id", "author") \
        .groupby("year", "subreddit_id") \
        .agg(F.collect_set(
                F.when(F.col("author") != "[deleted]", F.col("author"))
                ) \
            .alias("unique_authors"))

    # ratio = min(max(1, int(len(users) * 0.02)), 50)
    df_users_sampled = df_users_grouped.withColumn(
        "sampled_authors",
        F.slice(
            F.shuffle("unique_authors"),
            1,
            F.least(  # max is 50
                F.greatest(  # min is 1
                    F.floor(F.size("unique_authors") * 0.02).cast("int"),
                    F.lit(1)
                ),
                F.lit(50)
            )
        )
    )

    sampled_authors_flat = df_users_sampled.select("year", "subreddit_id",
        F.explode("sampled_authors").alias("sampled_authors")
    )


    # save sampled users
    sampled_authors_flat.coalesce(1).write\
        .mode("overwrite")\
        .option("header", "true") \
        .csv(f"{output_folder}/users")

    # join: keeps only sampled users
    df_final = df.join(df_users_sampled,
            (df.year == df_users_sampled.year) & \
            (df.subreddit_id == df_users_sampled.subreddit_id) & \
            F.array_contains(df_users_sampled.sampled_authors, df.author),
            how="inner") \
        .select(df['*'])

    for year in range(start_year, end_year + 1):
        df_year = df_final.filter(F.col("year") == year)
        df_year \
            .coalesce(1) \
            .write\
            .mode("overwrite")\
            .option("header", "true")\
            .parquet(f"{output_folder}/comments/users_comments_{year}")
        

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Parameters for sampling Reddit user comments")

    parser.add_argument("--start_year",
                         help="The first year from which to extract Reddit comments")
    parser.add_argument("--end_year",
                         help="The last year up to which Reddit comments are extracted")
    parser.add_argument("--input_folder",
                         help="Path to the folder containing input Reddit data files")
    parser.add_argument("--output_folder",
                         help="Path to the folder where the sampled comments are saved")
    
    args = parser.parse_args()
    run(args)