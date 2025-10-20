# the common sparksession as always
from pyspark.sql import SparkSession
import os 
os.environ["PYARROW_IGNORE_TIMEZONE"] = '1'
import pandas as pd 
# alias for pandas api on spark
import pyspark.pandas as ps

# Initialize SparkSession
spark = SparkSession.builder \
        .appName("PandasIntegrationPyspark") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
        .getOrCreate()

pandas_df = pd.DataFrame({
    "id": [1,2,3,4,5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45]
    })

# convert pandas dataframe to spark dataframe 
# this is not a pandas on spark dataframe
# this is a common spark dataframe that cabe from a pandas dataframe
spark_df = spark.createDataFrame(pandas_df)

spark_df.printSchema()


# filter
filtered_spark_df = spark_df.filter(spark_df.age > 30)

# back to simple pandas 
converted_pandas_df = filtered_spark_df.toPandas()
print(converted_pandas_df)


# now the real pandas on spark dataframe
ps_df = ps.DataFrame(pandas_df)

# pandas like operations but in pandas on spark dataframe
# this still uses the potential from spark distributed system 
print(ps_df['age'].max())


print(ps_df.describe())


