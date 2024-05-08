from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, year, round, max, sum, count
import os

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

words = spark.sparkContext.parallelize(text.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()

def init_spark(year):
    # Ensure the Spark session is available
    spark = SparkSession.builder.appName(f"SparkProcessing{year}").getOrCreate()
    input_file = f"/opt/airflow/scopus/output_{year}.json"

    # Check if the input file exists
    if not os.path.exists(input_file):
        print(f"No data file found for year {year}. Skipping processing.")
        spark.stop()
        return

    df = spark.read.json(input_file)
    df = df.select("id", "publicDate", "source", "citationCount", "coAuthorship", "refCount", "Class")  # Reordering for clarity

    # Cast data types as necessary
    df = df.withColumn('citationCount', col('citationCount').cast('int'))
    df = df.withColumn('coAuthorship', col('coAuthorship').cast('int'))
    df = df.withColumn('refCount', col('refCount').cast('int'))

    # Aggregate maximum values for normalization purposes
    max_values = df.agg(
        max(col("citationCount")).alias("maxCitation"),
        max(col("refCount")).alias("maxRef"),
        max(col("coAuthorship")).alias("maxCoAuthor")
    ).collect()[0]

    # Use the max values for normalization calculations
    max_citation = max_values["maxCitation"]
    max_ref = max_values["maxRef"]
    max_coauthor = max_values["maxCoAuthor"]

    if df.filter(col("Class").isNotNull()).count() > 0:
        df = df.withColumn("Class", explode(col("Class")))
    else:
        print("No classes to explode.")
        spark.stop()
        return

    df = df.dropna(subset=["id", "publicDate", "source", "citationCount", "coAuthorship", "refCount", "Class"])

    # Calculate scores
    df = df.withColumn(
        "Score",
        round(
            col("source") * (
                0.4 * (col("citationCount") / max_citation * 10) +
                0.2 * (col("refCount") / max_ref * 10) +
                0.1 * (col("coAuthorship") / max_coauthor * 10)
            ), 4
        )
    )

    df = df.withColumn("publicDate", to_date(col("publicDate"), "dd/MM/yyyy"))
    df = df.withColumn("Year", year(col("publicDate")))

    grouped_df = df.groupBy("Class", "Year").agg(
        round(sum("Score"), 4).alias("Total Score"),
        count("id").alias("Paper Count")
    )

    # Output the results to a CSV file
    output_path = f"/opt/spark/scopus/output_{year}.csv"
    grouped_df.write.csv(path=output_path, mode="overwrite", header=True)

    print(f"Data for year {year} processed and saved to {output_path}.")
    spark.stop()