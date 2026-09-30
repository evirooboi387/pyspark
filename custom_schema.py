from pyspark.sql.types import *


def custom(spark):
    # global schema
    schema = StructType() \
        .add("RecordNumber", IntegerType(), True) \
        .add("Zipcode", IntegerType(), True) \
        .add("ZipCodeType", StringType(), True) \
        .add("City", StringType(), True) \
        .add("State", StringType(), True) \
        .add("LocationType", StringType(), True) \
        .add("Lat", DoubleType(), True) \
        .add("Long", DoubleType(), True) \
        .add("Xaxis", IntegerType(), True) \
        .add("Yaxis", DoubleType(), True) \
        .add("Zaxis", DoubleType(), True) \
        .add("WorldRegion", StringType(), True) \
        .add("Country", StringType(), True) \
        .add("LocationText", StringType(), True) \
        .add("Location", StringType(), True) \
        .add("Decommisioned", BooleanType(), True) \
        .add("TaxReturnsFiled", StringType(), True) \
        .add("EstimatedPopulation", IntegerType(), True) \
        .add("TotalWages", IntegerType(), True) \
        .add("Notes", StringType(), True)

    df_with_schema = spark.read.format("csv").option("header", True).schema(schema).load(
        "file:///home/takeo/zipcodes.csv")
    df_with_schema.printSchema()
    df_with_schema.show()
    df_with_schema.write.mode('overwrite').csv("file:///tmp/spark_output/zipcodes")



