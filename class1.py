from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

from enforce_schema import nested_schema, enforce_schema, select, nested_structcolumns, withColumn, distinct, ordering, \
    car


def num_rdd():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    print(rdd.collect())

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    # num_rdd()

    # enforce_schema(spark)
    # nested_schema(spark)
    # select(spark)
    # nested_structcolumns(spark)
    # print("Before withcolumn")
    #
    # withColumn(spark)
    # print("After withcolumn")
    #
    # distinct(spark)
    # ordering(spark)
    car(spark)