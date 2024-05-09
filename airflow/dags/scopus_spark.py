from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, year, round as spark_round, max, sum as spark_sum, count, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType
import os
from pathlib import Path

schema = StructType([
    StructField("id", StringType(), True),
    StructField("publicDate", DateType(), True),
    StructField("source", IntegerType(), True),
    StructField("coAuthorship", IntegerType(), True),
    StructField("citationCount", IntegerType(), True),
    StructField("refCount", IntegerType(), True),
    StructField("Class", StringType(), True)
])


# Ensure the Spark session is available
year = 2018
spark = SparkSession.builder.appName(f"SparkProcessing{year}").getOrCreate()

# Print current working directory
current_directory = os.getcwd()
print(f"Current working directory: {current_directory}")

input_file = f"/opt/airflow/scopus/output_{year}.csv"
print(f"Attempting to read from: {input_file}")

# Check if the input file exists
csv_file = Path(input_file)
if csv_file.is_file():
    print("File exists, proceeding with reading.")
else:
    print("File not found, please check the path and permissions.")
    spark.stop()

# Load the CSV file with header
df = spark.read.schema(schema).option("header", "true").csv(input_file)

print(f"HELLO {23}")

print(df)

# Remove brackets and quotes, then split into array
df = df.withColumn("Class", regexp_replace(col("Class"), "[\\[\\]'']", ""))  # Remove [ ], and '
df = df.withColumn("Class", split(col("Class"), ",\\s*"))  # Split into array

print("ANOTHER:", df)

print(f"HELLO {123}")

df = df.dropDuplicates(["id"])

print(f"HELLO {213}")


# Cast data types as necessary
df = df.withColumn('citationCount', col('citationCount').cast('int'))
df = df.withColumn('coAuthorship', col('coAuthorship').cast('int'))
df = df.withColumn('refCount', col('refCount').cast('int'))

max_values = df.agg(
    max("citationCount").alias("maxCitation"),
    max("refCount").alias("maxRef"),
    max("coAuthorship").alias("maxCoAuthor")
).first()[0]

# Max value for each feature for normalization
max_citation = max_values["maxCitation"]
max_ref = max_values["maxRef"]
max_coauthor = max_values["maxCoAuthor"]

# Find all Class
genre_counts = df.withColumn("Genre", explode(col("Class")))\
                 .groupBy("Genre")\
                 .count()  

print('number of all the class:', genre_counts.count())

# Explode the class
exploded_df = df.withColumn("Class", explode(col("Class")))

# Drop na for invalid rows
cleaned_df = exploded_df.dropna()

# Compute the score for each paper
cleaned_df = cleaned_df.withColumn(
    "Score",
    spark_round(
        col("source") * (
            0.4 * (col("citationCount") / max_citation * 10) +
            0.2 * (col("refCount") / max_ref * 10) +
            0.1 * (col("coAuthorship") / max_coauthor * 10)
        ), 4
    )
)

# Convert publicDate from string to date type
cleaned_df = cleaned_df.withColumn("publicDate", to_date(col("publicDate"), "dd/MM/yyyy"))

# Extract Year and Quarter from publicDate
cleaned_df = cleaned_df.withColumn("Year", year(col("publicDate")))
cleaned_df = cleaned_df.withColumn("Quarter", quarter(col("publicDate")))

# Group by Class, Year, and Quarter and perform aggregations
grouped_df = cleaned_df.groupBy("Class", "Year", "Quarter").agg(
    spark_round(spark_sum("Score"), 4).alias("Total Score"),
    count("id").alias("Paper Count")  # Count the number of papers per group
)

# Coalesce the DataFrame to 1 partition to avoid multiple part files
grouped_df.coalesce(1).write.csv(path="./test_output.csv", mode="overwrite", header=True)

spark.stop()
