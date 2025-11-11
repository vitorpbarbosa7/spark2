from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf
import re

# -------------------------------------------------------------------
# 1️⃣ Initialize Spark Session
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("HashtagCountWithLateral")
    .config("spark.sql.execution.pythonUDTF.enabled", "true")
    .getOrCreate()
)

# -------------------------------------------------------------------
# 2️⃣ Define a Python UDTF (User Defined Table Function)
# -------------------------------------------------------------------
@udtf(returnType="hashtag STRING")
class HashtagExtractor:
    """Extracts hashtags (#word) from text and emits one row per hashtag"""
    def eval(self, text: str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for h in hashtags:
                yield (h,)

# -------------------------------------------------------------------
# 3️⃣ Register the UDTF for SQL usage
# -------------------------------------------------------------------
spark.udtf.register("extract_tags", HashtagExtractor)

# -------------------------------------------------------------------
# 4️⃣ Sample DataFrame (tweets)
# -------------------------------------------------------------------
data = [
    ("Loving #ApacheSpark and #BigData",),
    ("Exploring #AI and #ML with #Spark",),
    ("No hashtags here",),
    ("Another tweet with #AI",)
]
df = spark.createDataFrame(data, ["text"])
df.createOrReplaceTempView("tweets")

# -------------------------------------------------------------------
# 5️⃣ Run SQL Query using LATERAL join and aggregate
# -------------------------------------------------------------------
print("\n🔹 Hashtag frequencies using LATERAL and GROUP BY:")
result = spark.sql("""
    SELECT hashtag, COUNT(*) AS freq
    FROM tweets, LATERAL extract_tags(text)
    GROUP BY hashtag
    ORDER BY freq DESC
""")

result.show(truncate=False)

# -------------------------------------------------------------------
# 6️⃣ Stop Spark session
# -------------------------------------------------------------------
spark.stop()

