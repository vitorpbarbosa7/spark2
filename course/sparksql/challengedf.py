from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.option("inferSchema", "true")\
        .csv("fakefriends.csv")


columns = ["id","name","age","friends"]
df = df.toDF(*columns)

print("Here is our inferred schema:")
df.printSchema()

print("let's display the name column:")
df.select("name").show()

print("Filter out anyone over 21:")
df.filter(df.age < 21).show()

print("Group by age")
df.groupBy("age").count().show()

print("Make everyone 10 years older:")
df.select(df.name, df.age + 10).show()

spark.stop()
