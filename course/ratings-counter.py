from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("ml-100k/u.data")
# for each line, split and get the third element
ratings = lines.map(lambda x: x.split()[2])
# countByValue implementation
result = ratings.countByValue()

print(type(result))
print(dir(result))

# to sort the results
sortedResults = collections.OrderedDict(sorted(result.items()))
# once a dictionary is returned, therefore 
for key, value in sortedResults.items():
    print("%s %i" % (key,value))