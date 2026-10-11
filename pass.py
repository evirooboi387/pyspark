from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, count, sum, col, when, lit


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



# 1. Select specific columns
df_selected = df.select("Name", "Age")

# 2. Filter rows based on conditions
df_filtered = df.filter(col("Age") > 30)

# 3. Add or modify a column (withColumn)
df_with_tax = df.withColumn("Salary_Multiplier", col("Age") * 1000)

# 4. Conditional statement (when / otherwise)
df_lifecycle = df.withColumn(
    "Age_Group",
    when(col("Age") < 30, "Young Adult")
    .when(col("Age").between(30, 40), "Middle Aged")
    .otherwise("Senior")
)

df_lifecycle.show()



# Group by Country and calculate average age and volume count
df_grouped = df.groupBy("Country").agg(
    avg("Age").alias("Average_Age"),
    count("Name").alias("Total_People")
)

df_grouped.show()

