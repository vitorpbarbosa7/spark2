# they know each other if they appear in the same comic book 


# split off one hero Id beginning of line
# count how many space-separated numbers are in the line 
# group by hero id's to add up connections split into multiple lines (sum(other_heros_count) group by hero_id) 

# sort by total connections

# broadcast?
# filter name lookup dataset by the most popular hero Id to look up the name

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperHero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("marvelnames")
lines = spark.read.text("marvelgraph")

# Both here are already dataframes
print(type(names))
print(type(lines))


# by default the name of the column is value
lines.printSchema()
lines.show(10, truncate=False)
print("✅ TYPE:", type(lines))
print("📋 COLUMNS:", lines.columns)
print("📏 NUM COLUMNS:", len(lines.columns))
print("🧮 NUM ROWS:", lines.count())

print("\n🔍 SCHEMA:")
lines.printSchema()

print("\n🧠 DTYPES:")
print(lines.dtypes)

print("\n📊 DESCRIBE:")
lines.describe().show()

print("\n⚙️ PLAN:")
lines.explain(True)




# This creates a dataframe
    # split it all, and get first element, call this column "id"
    # split it all and count how many there are, subtracting 1
    # after, group it by id, and sum the name of connections that there is
connections = \
    lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) 
    
connectionsGroupBy = \
    connections.groupBy("id").agg(func.sum("connections").alias("connections"))

print("Connections")
connections.show(10, truncate=True)
print("##### ConnectionsGroupBy ######")
connectionsGroupBy.show(10, truncate=True)


mostPopular = connectionsGroupBy.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id")==mostPopular[0]).select("name").first()

print("-------------Final Result--------------")
print(mostPopularName[0])
print(mostPopular[1])