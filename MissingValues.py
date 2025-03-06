from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType, DoubleType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()

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
df.na.drop(subset=['year']).show()

# how all: both columns are missing ge remove. any: any column is missing ge removed.
df.na.drop(how='all', subset=['year', 'pm']).show()
df.na.drop(how='any', subset=['year', 'pm']).show()

# 2. fill
# fill missing value with default value
df.na.fill(50.0, subset=['pm']).show(100000)

# 3. conditional replace

df = spark.read \
    .option('header', True) \
    .csv('dataset/BeijingPM20100101_20151231.csv')

result = df.withColumn('pm', when(col('PM_Dongsi') == 'NA', 20.0)
              .otherwise(col('PM_Dongsi').cast(DoubleType()))
              )

result.show(100000)