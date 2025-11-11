from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, lit
from typing import Iterator, Tuple
import json

spark = SparkSession.builder.appName("UDTFExample").getOrCreate()

# --- UDTF handler must be a class with eval() ---
class ParseEvents:
    def eval(self, json_str: str) -> Iterator[Tuple[str, str]]:
        data = json.loads(json_str or "{}")
        user_id = data.get("user", "unknown")
        for event in data.get("events", []):
            yield (user_id, event)

# Wrap the class as a UDTF object
parse_events = udtf(ParseEvents, returnType="user_id STRING, event STRING")

# Option A: use in SQL
df_sql = spark.sql("""
    SELECT * FROM parse_events('{"user":"U123","events":["click","view","purchase"]}')
""")
df_sql.show(truncate=False)
#
## Option B: use in the DataFrame API
#df_api = parse_events(lit('{"user":"U123","events":["click","view","purchase"]}'))
#df_api.show(truncate=False)
#
spark.stop()

