from pyspark.sql import SparkSession 
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# to lowercase

lowercase = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# count the ocurrence of each word 
wordCounts = lowercase.groupby("word").count()

# sort by counts
wordCountsSorted = wordCounts.sort("count")

# show results
# inside function shows number of rows
# .show allows to show all of those rows
wordCountsSorted.show(wordCountsSorted.count())
