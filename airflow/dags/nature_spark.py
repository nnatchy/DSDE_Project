import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, year as spark_year, round, max, sum, count, split, regexp_replace, quarter
from scopus_spark import create_spark_session, read_data, prepare_and_process_dataframe, write_results

def process_nature_dataset(input_file, output_file, app_name):
    spark = create_spark_session(app_name)
    df = read_data(spark, input_file)
    if df is not None:
        df = prepare_and_process_dataframe(df)
        write_results(df, output_file)
    spark.stop()

if __name__ == '__main__':
    process_nature_dataset("out/nature/output_nature.csv", "out/nature/spark_output_nature.csv", "SparkProcessingNature")