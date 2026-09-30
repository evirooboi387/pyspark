def readfunc(spark):
    df = spark.read.csv("file:///home/takeo/zipcodes.csv")
    df.printSchema()
    df.show(truncate=False)
    df2= spark.read.option("header",True).csv("file:///home/takeo/zipcodes.csv")
    df2.show()
