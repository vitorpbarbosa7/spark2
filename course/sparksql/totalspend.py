from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True), \
    StructField("item", IntegerType(), True), \
    StructField("value", FloatType(), True)])

df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()


dfSumOrder = df.groupBy("id").sum("value")
dfSumOrder.show()

dfAgg = df.groupBy("id").agg(func.round(func.sum("value"), 2).alias("total_spent"))

dfAgg = dfAgg.sort("total_spent")
dfAgg.show()


spark.stop()

