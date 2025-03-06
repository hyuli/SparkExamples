from pyspark import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year, when, avg, count, max, min, split, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType, DoubleType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()

# 1) get hive context
hc = HiveContext(spark.sparkContext)

b_df = hc.table('business')

# 1. Most common bussiness
result = b_df.groupBy('name')\
    .agg(count('name').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(20)

# 2. Most common category
result = b_df.select(explode(split('categories', ', ')).alias('category'))\
    .groupBy('category')\
    .agg(count('category').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(50)
