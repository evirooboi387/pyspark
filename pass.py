from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Examples") \
    .master("local[*]") \
    .getOrCreate()


# Method 1: Infer schema automatically
data = [("Alice", 34, "USA"), ("Bob", 45, "Canada"), ("Catherine", 29, "UK")]
columns = ["Name", "Age", "Country"]
df = spark.createDataFrame(data, columns)

# Method 2: Define explicit schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Country", StringType(), True)
])
df_with_schema = spark.createDataFrame(data, schema=schema)

df.show()
# Read from a CSV file
df_csv = spark.read.csv("path/to/sample.csv", header=True, inferSchema=True)

# Write DataFrame to a Parquet file
df_csv.write.mode("overwrite").parquet("path/to/output_data.parquet")

# Read from the Parquet file
df_parquet = spark.read.parquet("path/to/output_data.parquet")
