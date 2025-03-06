from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()


# Read data
# 1. text file
spark.read.text('dataset/LICENSE.txt').show()
spark.read.format('text').load('dataset/LICENSE.txt').show()

spark.read.text('hdfs://node-master:9000/test.txt').show()
# 2. csv file

# 3. json

# 4. jdbc: from database

# 5. orc/ parquest: compressed format for hadoop

# 6. table: Hive

# Write data

df = spark.read\
    .option('header', True)\
    .csv('dataset/BeijingPM20100101_20151231.csv')

df.write\
    .mode('overwrite')\
    .json('output')