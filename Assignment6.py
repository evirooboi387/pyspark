from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()

    # Question1
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    rdd = spark.sparkContext.parallelize(data, 5)

    print("Number of partitions:", rdd.getNumPartitions())


# Question2
rdd1 = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/sparkproject/records.txt")

print("Number of records:", rdd1.count())

# Question3
para_graph = "Python Lists allow us to hold items of heterogeneous types. In this article, we will learn how to create a list in Python; access the list items; find the number of items in the list, how to add an item to list; how to remove an item from the list; loop through list items; sorting a list, reversing a list; and many more transformation and aggregation actions on Python Lists."

rdd2=spark.sparkContext.parallelize([para_graph])

word_rdd= rdd2.flatMap(lambda x: x.split())

word_count= word_rdd.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

for word, count in word_count.collect():
    print((word, count))

# Question4
data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()

# Register DataFrame as SQL view
df.createOrReplaceTempView("Users")

# SQL query
result = spark.sql("""
    SELECT *
    FROM Users
    WHERE salary > 3000
""")

result.show()


# Question5
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
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
df = spark.createDataFrame(structureData, structureSchema)

df.show()

# Register DataFrame as SQL view
df.createOrReplaceTempView("Users")

# SQL query
result = spark.sql("""
    SELECT name.firstname
    FROM Users
    WHERE name.lastname = 'Rose'
""")

result.show()
