from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year, when, avg, count, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType, DoubleType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()


person_df = spark.sparkContext.parallelize([(0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0)])\
  .toDF(["id", "name", "cityId"])

city_df = spark.sparkContext.parallelize([(0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou")]) \
  .toDF(["id", "name"])


person_df.show()
city_df.show()

person_df.join(city_df, person_df['cityId'] == city_df['id'])\
    .select(person_df['id'], person_df['name'], city_df['name'].alias('city_name')).show()