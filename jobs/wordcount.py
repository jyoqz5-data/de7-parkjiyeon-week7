from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/opt/spark/data/wordcount.txt"

spark = (
    SparkSession.builder
    .appName("Q4-WordCount")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

words = (
    spark.read.text(INPUT_PATH)
    .select(
        F.explode(
            F.split(F.col("value"), r"\s+")
        ).alias("word")
    )
    .filter(F.col("word") != "")
)

word_counts = (
    words.groupBy("word")
    .count()
    .orderBy(F.desc("count"), F.asc("word"))
)

print("=== TOP 20 WORD COUNTS ===")
word_counts.show(20, truncate=False)

spark.stop()