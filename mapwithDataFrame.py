def mapDataFrame(spark):
    global columns
    data = [('James', 'Smith', 'M', 30),
            ('Anna', 'Rose', 'F', 41),
            ('Robert', 'Williams', 'M', 62),
            ]

    columns = ["firstname", "lastname", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show()

