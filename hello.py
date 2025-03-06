# Step1. Create spark session
from pyspark.sql import SparkSession

#Alt+Enter fix problem for u

# master: local: Running in my local yarn: run in yarn cluster, mesos: 7077: standalone
spark = SparkSession.builder\
    .appName("Spark Example")\
    .master("local")\
    .getOrCreate()

# Step2. submit analysis to spark session created in step 1.

df = spark.createDataFrame([('tom', 20), ('jack', 40)], ['name', 'age'])

print(df.count())

# Step3. Close spark session

spark.stop()

print('Modified by B')

print('modified by A')