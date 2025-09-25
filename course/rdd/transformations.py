from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDDExamples").getOrCreate()
sc = spark.sparkContext

print("\n--- Base RDD ---")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
print("Original:", rdd.collect())

# 1. map
print("\n--- map ---")
print(rdd.map(lambda x: x * 2).collect())

# 2. flatMap
print("\n--- flatMap ---")
words = sc.parallelize(["hello world", "spark rdd"])
print(words.flatMap(lambda line: line.split(" ")).collect())

# 3. filter
print("\n--- filter ---")
print(rdd.filter(lambda x: x % 2 == 0).collect())

# 4. distinct
print("\n--- distinct ---")
dup = sc.parallelize([1, 2, 2, 3, 3, 3])
print(dup.distinct().collect())

# 5. sample
print("\n--- sample ---")
print(rdd.sample(False, 0.5, seed=42).collect())

# 6. union
print("\n--- union ---")
a = sc.parallelize([1, 2, 3])
b = sc.parallelize([3, 4, 5])
print(a.union(b).collect())

# 7. intersection
print("\n--- intersection ---")
print(a.intersection(b).collect())

# 8. subtract
print("\n--- subtract ---")
print(a.subtract(b).collect())

# 9. cartesian
print("\n--- cartesian ---")
print(a.cartesian(b).collect())
