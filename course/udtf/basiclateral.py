from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

spark = SparkSession.builder \
    .appName("LateralJoinExample") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

@udtf(returnType="word STRING")
class WordSplitter:
    def eval(self, text: str):
        for w in text.split():
            yield (w,)

spark.udtf.register("split_words", WordSplitter)

data = [("I love #ApacheSpark",), ("Big data rocks",)]
df = spark.createDataFrame(data, ["text"])
df.createOrReplaceTempView("docs")

spark.sql("""
select text
from 
docs
"""
).show(truncate=False)


# word is the same name as the return type from the udtf
spark.sql("""
SELECT text, word
FROM docs, LATERAL split_words(text)
""").show(truncate=False)

