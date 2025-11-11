from pyspark.sql import SparkSession
import pyspark.pandas as ps
# Must set this env variable to avoid warnings

import os
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"   # <- key fix
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"                 # you already had this

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("PoS")
    .config("spark.executorEnv.OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)


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
print(ps_df.to_spark().show(5))

# transform 
ps_df['age10years'] = ps_df['age'].transform(lambda x: x + 10)
print(ps_df.to_spark().show(5))


# custom function

def categorize_salary(salary):
    if salary < 60000:
        return "low"
    elif salary < 100000:
        return "medium"
    else:
        return "high"

# apply
ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)
print(ps_df.to_spark().show(5))

