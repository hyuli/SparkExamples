from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year, when, avg, count, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType, DoubleType

# spark = SparkSession.builder \
#     .appName('DataTypeExample') \
#     .master('local') \
#     .getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("hour", IntegerType()),
    StructField("season", IntegerType()),
    StructField("pm", DoubleType())
])

df = spark.read.schema(schema) \
    .option('header', True) \
    .csv('/dataset/beijingpm_with_nan.csv')

clean_df = df.na.drop(subset=['pm'])

# clean_df.agg(avg('pm'), count('pm'), max('pm'), min('pm')).show()
#
# clean_df.groupBy('year') \
#     .agg(avg('pm'), count('pm'), max('pm'), min('pm')) \
#     .show()

clean_df.groupBy('month')\
    .agg(avg('pm').alias('avg_pm'))\
    .orderBy(col('avg_pm').desc())\
    .show()