from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp, year
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, BooleanType, \
    FloatType

spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

df = spark.read \
    .option('header', True) \
    .schema(fire_schema) \
    .csv('dataset/sf-fire-calls.txt')

df.printSchema()
df.show()

# 1) Select "IncidentNumber", "AvailableDtTm", "CallType" from data, and filter the CallType is not "Medical Incident"
df.select('IncidentNumber', 'AvailableDtTm', 'CallType') \
    .where(col('CallType') != 'Medical Incident') \
    .show()

# 2) how many distinct CallTypes.
cnt = df.select('CallType') \
    .where(col('CallType').isNotNull()) \
    .distinct() \
    .count()

print(f'The total number of call types is {cnt}')

# 3) Rename Column Delay to "ResponseDelayedinMins" and and take a look at the response times that were longer than five minutes:
df.withColumnRenamed('Delay', 'ResponseDelayedinMins') \
    .where(col('ResponseDelayedinMins') > 5) \
    .show()

# 4) Convert "CallDate", "WatchDate" and "AvailableDtTm" to timestamp date type
clean_df = df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yyyy')) \
    .drop('CallDate')

# 5) which year has the most fire calls.
clean_df.withColumn('year', year('IncidentDate')) \
    .groupBy('year') \
    .count() \
    .orderBy(col('count').desc()) \
    .show()
