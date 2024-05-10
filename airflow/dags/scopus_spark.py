import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, year as spark_year, round, max, sum, count, split, regexp_replace, quarter, concat_ws

def create_spark_session(app_name):
    """ Create and return a Spark session. """
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, input_file):
    """ Check if the file exists and read data from CSV. """
    if not spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(input_file)):
        print("File not found, please check the path and permissions.")
        spark.stop()
        return None
    return spark.read.option("header", "true").csv(input_file)

def prepare_and_process_dataframe(df):
    """ Prepare the dataframe by cleaning data, exploding and computing scores. """
    df = df.withColumn("Class", regexp_replace(col("Class"), "[\\[\\]'']", ""))
    df = df.withColumn("Class", split(col("Class"), ",\\s*"))
    df = df.dropDuplicates(["id"])
    df = df.withColumn('citationCount', col('citationCount').cast('int'))
    df = df.withColumn('coAuthorship', col('coAuthorship').cast('int'))
    df = df.withColumn('refCount', col('refCount').cast('int'))

    # Aggregating max values for normalization
    max_values = df.agg(
        max("citationCount").alias("maxCitation"),
        max("refCount").alias("maxRef"),
        max("coAuthorship").alias("maxCoAuthor")
    ).collect()[0]

    df = df.withColumn("Class", explode(col("Class")))
    df = df.withColumn(
        "Score",
        round(
            col("source") * (
                0.4 * (col("citationCount") / max_values["maxCitation"] * 10) +
                0.2 * (col("refCount") / max_values["maxRef"] * 10) +
                0.1 * (col("coAuthorship") / max_values["maxCoAuthor"] * 10)
            ), 4
        )
    )
    return df

def write_results(df, output_file):
    """ Write results to CSV after additional transformations. """
    df = df.withColumn("publicDate", to_date(col("publicDate"), "dd/MM/yyyy"))
    df = df.withColumn("Year", spark_year(col("publicDate")))
    df = df.withColumn("Quarter", quarter(col("publicDate")))
    grouped_df = df.groupBy("Class", "Year", "Quarter").agg(
        round(sum("Score"), 4).alias("Total Score"),
        count("id").alias("Paper Count")
    )
    grouped_df.coalesce(1).write.csv(path=output_file, mode="overwrite", header=True)

def process_scopus_dataset(input_file, output_file, app_name):
    spark = create_spark_session(app_name)
    df = read_data(spark, input_file)
    if df is not None:
        df = prepare_and_process_dataframe(df)
        write_results(df, output_file)
    spark.stop()

if __name__ == '__main__':
    year = sys.argv[1] if len(sys.argv) > 1 else 2022
    process_scopus_dataset(f"out/scopus/output_scopus_{year}.csv", f"out/scopus/spark_output_{year}.csv", "SparkProcessingScopus")
