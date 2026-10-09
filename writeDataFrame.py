def rwparquet(spark):
    global columns
    data = [("James ", "", "Smith", "36636", "M", 3000),
            ("Michael ", "Rose", "", "40288", "M", 4000),
            ("Robert ", "", "Williams", "42114", "M", 4000),
            ("Maria ", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    df=spark.createDataFrame(data, columns)
    df.write.parquet("file:///tmp/output/people.parquet")
    parDF=spark.read.parquet("file:///tmp/output/people.parquet")
    parDF.createOrReplaceTempView("ParquetTable")
    parkSQL= spark.sql("select * from ParquetTable where salaey >= 4000")
    parkSQL.show()
