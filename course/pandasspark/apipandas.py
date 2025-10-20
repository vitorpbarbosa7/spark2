from pyspark.sql import SparkSession 

import os 
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps

# Initialize Spark Session

spark = SparkSession.builder \
    .appName("Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# pandas on spark dataframe directly from data
ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})


# age
print(ps_df['age'].mean())


# statistics:
print(ps_df.describe())

print(ps_df.dtypes)
