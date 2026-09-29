def mapDataFrame(spark):
    global columns
    data = [('James', 'Smith', 'M', 30),
            ('Anna', 'Rose', 'F', 41),
            ('Robert', 'Williams', 'M', 62),
            ]

    columns = ["firstname", "lastname", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show()
#     refering columns by index
    rdd2=df.rdd.map(lambda x:
    (x[0]+","+x[1],x[2],x[3]*2)
    )
    df2=rdd2.toDF(["name","gender","new_salary"])
    df2.show()