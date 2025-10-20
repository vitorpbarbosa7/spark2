from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps  # Import pandas-on-Spark


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Transform and Apply in Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# Create a pandas-on-Spark DataFrame

ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})

print("Original Pandas-on-Spark DataFrame:")
print(ps_df)

# transform 
ps_df['age10years'] = ps_df['age'].transform(lambda x: x + 10)
print(ps_df.head(5))
