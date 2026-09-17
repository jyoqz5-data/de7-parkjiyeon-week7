import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-year", type=int, default=2015)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("q9-netflix-transform")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        source_df = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(args.input)
        )
        input_record_count = source_df.count()
        print(f"INPUT_RECORD_COUNT={input_record_count}", flush=True)

        required_columns = {"type", "release_year", "listed_in"}
        missing_columns = required_columns - set(source_df.columns)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        exploded_df = (
            source_df
            .withColumn(
                "release_year_int",
                F.col("release_year").cast("int"),
            )
            .filter(F.col("release_year_int") >= F.lit(args.min_year))
            .withColumn(
                "genre",
                F.explode(F.split(F.col("listed_in"), ",")),
            )
            .withColumn("genre", F.trim(F.col("genre")))
            .filter(F.col("genre").isNotNull())
            .filter(F.col("genre") != "")
        )

        result_df = (
            exploded_df
            .groupBy("type", "genre")
            .agg(F.count("*").alias("count"))
            .orderBy("type", "genre")
        )

        aggregated_row_count = result_df.count()

        print(f"MIN_RELEASE_YEAR={args.min_year}", flush=True)
        print(
            f"AGGREGATED_ROW_COUNT={aggregated_row_count}",
            flush=True,
        )

        (
            result_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(args.output)
        )

        print(f"PARQUET_OUTPUT={args.output}", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
