from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year, when, avg, count, max, min, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType, DoubleType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()

# builtint functions
# User defined function(UDF)


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
    .csv('dataset/beijingpm_with_nan.csv')

# 1. drop
# subset: column to check missing value
clean_df = df.na.drop(subset=['pm'])

func = lambda x: x * 2

double_func = udf(func) # register the function

clean_df.withColumn('double pm', double_func('pm')).show()
