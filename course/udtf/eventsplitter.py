from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

spark = SparkSession.builder \
    .appName("LateralJoinExample") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

from pyspark.sql.functions import udtf
import json

@udtf(returnType="user STRING, event STRING")
class EventParser:
    def eval(self, json_str: str):
        data = json.loads(json_str)
        user = data.get("user")
        for e in data.get("events", []):
            yield (user, e)

spark.udtf.register("parse_events", EventParser)

json_data = [
    ('{"user":"u1","events":["click","view"]}',),
    ('{"user":"u2","events":["scroll"]}',)
]
df = spark.createDataFrame(json_data, ["raw"])
df.createOrReplaceTempView("raw_table")

spark.sql("""
SELECT user, event
FROM raw_table, LATERAL parse_events(raw)
""").show()

