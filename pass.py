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
