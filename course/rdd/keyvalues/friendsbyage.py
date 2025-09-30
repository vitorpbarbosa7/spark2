from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("people.csv")
rdd = lines.map(parseLine)

# cria a tupla chave valor
# (valor, contador)
# soma valores
# soma quantidade

mapValueCount = rdd.mapValues(lambda value: (value, 1))
# sum each ones with same key (the count and the value)
totalsByAge = mapValueCount.reduceByKey(lambda value, count: (value[0] + value[0], count[1] + count[1]))
averagesByAge = totalsByAge.mapValues(lambda value: value[0] / value[1])

print("mapValueCount:", mapValueCount.collect())
print("totalsByAge:", totalsByAge.collect())
print("averagesByAge:", averagesByAge.collect())

results = averagesByAge.collect()
for result in results:
    print(result)
