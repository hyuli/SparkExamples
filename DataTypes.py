from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType

spark = SparkSession.builder\
    .appName('DataTypeExample')\
    .master('local')\
    .getOrCreate()

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("First", StringType(), True),
    StructField("Last", StringType(), True),
    StructField("Url", StringType(), True),
    StructField("Published", StringType(), True),
    StructField("Hits", LongType(), True),
    StructField("Campaigns", ArrayType(StringType()), True),
])

df = spark.read\
    .schema(schema)\
    .json('dataset/blogs.txt')

df.printSchema()
df.show()