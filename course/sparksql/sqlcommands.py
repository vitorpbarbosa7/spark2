from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# user defined functions (udf)
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
            age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

# create manually the dataframe from reading each row
schemaPeople = spark.createDataFrame(people).cache()

# creating the tempview to create queries on it
schemaPeople.createOrReplaceTempView("people")

# now we can use sql
teenagers = spark.sql("select * from people where age >= 13 and age <= 19")

# ver as primeiras linhas em formato tabular (distribuído, sem trazer tudo pro driver)
teenagers.show(20, truncate=False)

# ver o schema
teenagers.printSchema()

# ver um sample menor (mais seguro que collect)
teenagers.limit(10).show()

# result of a sql is an rdd and support all the normal rdd operations
for teen in teenagers.collect():
    print(teen)


spark.stop()




