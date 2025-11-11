# spark_smoketest.py
import os
# macOS niceties to avoid Arrow/fork quirks during simple prints
os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

from pyspark.sql import SparkSession, functions as F

# Start Spark
spark = (
    SparkSession.builder
    .appName("Spark4_PySpark_SmokeTest")
    .getOrCreate()
)

print("\n=== Spark version ===")
print(spark.version)  # should print 4.0.1 (or your 4.0.x)

# Simple DataFrame
df = spark.createDataFrame(
    [(1, "Alice", 25),
     (2, "Bob",   30),
     (3, "Cara",  35)],
    ["id", "name", "age"]
)

print("\n=== DataFrame.show() ===")
df.show()

# Simple transform + aggregation
print("\n=== Transform + Aggregation ===")
out = (df
       .withColumn("age_plus_5", F.col("age") + F.lit(5))
       .groupBy()
       .agg(F.count("*").alias("rows"),
            F.avg("age_plus_5").alias("avg_age_plus_5")))
out.show()

# SQL roundtrip
df.createOrReplaceTempView("people")
print("\n=== SQL ===")
spark.sql("""
  SELECT name, age, CASE WHEN age >= 30 THEN '30+' ELSE '<30' END AS bucket
  FROM people
  ORDER BY age
""").show()

# RDD quick check
print("\n=== RDD ===")
nums = spark.sparkContext.parallelize(range(1, 11))  # 1..10
print("sum(1..10) =", nums.reduce(lambda a, b: a + b))  # expect 55

spark.stop()
print("\n✓ Smoke test finished OK.\n")

