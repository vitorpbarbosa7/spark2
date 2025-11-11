from pyspark.sql import SparkSession

from pyspark.sql.functions import udtf, udf

from pyspark.sql.types import IntegerType

import re

# Initialize Spark session

spark = SparkSession.builder \
    .appName("Python UDTF and UDF Example") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()
from pyspark.sql.functions import udtf

@udtf(returnType="word: string")
class WordSplitter:
    def eval(self, text: str):
        for word in text.split(" "):
            yield (word.strip(),)

# Register the UDTF for use in Spark SQL.
spark.udtf.register("split_words", WordSplitter)

# Example: Using the UDTF in SQL.
spark.sql("SELECT * FROM split_words('hello world')").show()
# +-----+
# | word|
# +-----+
# |hello|
# |world|
# +-----+

# Example: Using the UDTF with a lateral join in SQL.
# The lateral join allows us to reference the columns and aliases
# in the previous FROM clause items as inputs to the UDTF.
spark.sql(
    "SELECT * FROM VALUES ('Hello World'), ('Apache Spark') t(text), "
    "LATERAL split_words(text)"
).show()
# +------------+------+
# |        text|  word|
# +------------+------+
# | Hello World| Hello|
# | Hello World| World|
# |Apache Spark|Apache|
# |Apache Spark| Spark|
# +------------+------+

