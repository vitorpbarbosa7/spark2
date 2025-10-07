import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("book.txt")
words = input.flatMap(normalizeWords)

# transform into key value pair 
# sum the values using the keys
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# sort by key 
# put the value first, instead of the key 
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    # count is the first part (swapped)
    count = str(result[0])
    # word is the second (swapped)
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
