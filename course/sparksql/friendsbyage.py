from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.option("inferSchema", "true").csv("fakefriends.csv")

columns = ["id","name", "age", "friends"]
df = df.toDF(*columns)

df.groupBy("age").avg("friends").show()
df.groupBy("age").count().sort("age").show()

spark.stop()
