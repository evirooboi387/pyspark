from os import truncate

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

def enforce_schema(spark):
    data = [("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
            ]

    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show()

def nested_schema(spark):
    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])

    df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
    df2.printSchema()
    df2.show(truncate=False)


def select(spark):
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)
    df.select(df.firstname, df.lastname).show()
    df.select(df.columns[:3]).show(3)


def nested_structcolumns(spark):
    data = [
        (("James", None, "Smith"), "OH", "M"),
        (("Anna", "Rose", ""), "NY", "F"),
        (("Julia", "", "Williams"), "OH", "F"),
        (("Maria", "Anne", "Jones"), "NY", "M"),
        (("Jen", "Mary", "Brown"), "NY", "M"),
        (("Mike", "Mary", "Williams"), "OH", "M")
    ]

    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])

    df2 = spark.createDataFrame(data=data, schema=schema)
    df2.printSchema()
    df2.show(truncate=False)  # shows all columns
    df2.select("name").show(truncate=False)


