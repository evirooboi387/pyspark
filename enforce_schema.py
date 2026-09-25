from os import truncate

from pyspark.sql.functions import col,sum,avg,max,min,mean,count
from pyspark.sql.functions import expr
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
    df.select(col("firstname"), col("lastname")).show()
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


def withColumn(spark):
    data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
            ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    ddf = df.withColumn("salary", col("salary").cast("Double"))
    ddf.show()
    ddf.printSchema()
    udf = df.withColumn("salary", col("salary") * 100)
    udf.show()


def distinct(spark):
    global columns
    data = [("James", "Sales", 3000), \
            ("Michael", "Sales", 4600), \
            ("Robert", "Sales", 4100), \
            ("Maria", "Finance", 3000), \
            ("James", "Sales", 3000), \
            ("Scott", "Finance", 3300), \
            ("Jen", "Finance", 3900), \
            ("Jeff", "Marketing", 3000), \
            ("Kumar", "Marketing", 2000), \
            ("Saif", "Sales", 4100) \
            ]
    columns = ["employee_name", "department", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)
    distinctDF = df.distinct()
    print("Distinct count: " + str(distinctDF.count()))
    distinctDF.show(truncate=False)

    df2 = df.dropDuplicates()
    print("Distinct count: " + str(df2.count()))
    df2.show(truncate=False)

    dropDisDF = df.dropDuplicates(["department", "salary"])
    print("Distinct count of department & salary : " + str(dropDisDF.count()))
    dropDisDF.show(truncate=False)


def ordering(spark):
    global columns
    simpleData = [("James", "Sales", "NY", 90000, 34, 10000), \
                  ("Michael", "Sales", "NY", 86000, 56, 20000), \
                  ("Robert", "Sales", "CA", 81000, 30, 23000), \
                  ("Maria", "Finance", "CA", 90000, 24, 23000), \
                  ("Raman", "Finance", "CA", 99000, 40, 24000), \
                  ("Scott", "Finance", "NY", 83000, 36, 19000), \
                  ("Jen", "Finance", "NY", 79000, 53, 15000), \
                  ("Jeff", "Marketing", "CA", 80000, 25, 18000), \
                  ("Kumar", "Marketing", "NY", 91000, 50, 21000) \
                  ]
    columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=simpleData, schema=columns)
    df.printSchema()
    df.show(truncate=False)
    df.sort("department","state").show(truncate=False)
    # df.sort(col("department"),col("state")).show(truncate=False)
    print("Before ordering")
    df.sort(df.department.asc(), df.state.asc()).show(truncate=False)

    df.orderBy("department","state").show(truncate=False)
    print("After ordering")


def car(spark):
    global columns
    data = [
        ("Ford Torino", 140, 3449, "US"), \
        ("Chevrolet Monte Carlo", 150, 3761, "US"), \
        ("BMW 2002", 113, 2234, "Europe") \
        ]
    columns = ["car", "horsepower", "weight", "origin"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    # df.show(truncate=False)
    df.agg(avg("weight")).show()


