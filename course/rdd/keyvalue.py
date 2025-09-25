from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MapValuesExamples").getOrCreate()
sc = spark.sparkContext

print("\n--- Example 1: mapValues ---")
rdd = sc.parallelize([
    ("Alice", 80),
    ("Bob", 90),
    ("Alice", 85),
    ("Bob", 95)
])

# Add +5 to each grade
# access the value, not the key
curved = rdd.mapValues(lambda grade: grade + 5)
print(curved.collect())
# [('Alice', 85), ('Bob', 95), ('Alice', 90), ('Bob', 100)]


print("\n--- Example 2: flatMapValues ---")
rdd2 = sc.parallelize([
    ("Alice", ["Math", "History"]),
    ("Bob", ["Biology"]),
    ("Charlie", ["Physics", "Chemistry", "Math"])
])

# Expand subjects into multiple rows
expanded = rdd2.flatMadValues(lambda subjects: subjects)
print(expanded.collect())
# [('Alice', 'Math'), ('Alice', 'History'),
#  ('Bob', 'Biology'),
#  ('Charlie', 'Physics'), ('Charlie', 'Chemistry'), ('Charlie', 'Math')]

spark.stop()

