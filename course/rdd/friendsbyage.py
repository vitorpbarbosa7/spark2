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

mapValueCount = rdd.mapValues(lambda x: (x, 1))
totalsByAge = mapValueCount.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

print("mapValueCount:", mapValueCount.collect())
print("totalsByAge:", totalsByAge.collect())
print("averagesByAge:", averagesByAge.collect())

results = averagesByAge.collect()
for result in results:
    print(result)


