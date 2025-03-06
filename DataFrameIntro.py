from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('DataFrameIntro')\
    .master('local')\
    .getOrCreate()

# 1) spark session is entrance for spark SQL
# 2) spark context for RDD
# get spark context for spark session
# parallelize -> create rdd
rdd = spark.sparkContext.parallelize([("tom", 3), ("jerry", 1)])
df = rdd.toDF(["name", "age"])

#1) print schema definition
df.printSchema()

# print rows
df.show()

#1. Create from memory, most of time its for test

# rdd = spark.sparkContext.parallelize([("tom", 3), ("jerry", 1)])

#2. create from by reading files/ databases
# header -> True head line is column def
df = spark.read\
    .option('header', True)\
    .csv('dataset/BeijingPM20100101_20151231.csv')

df.printSchema()

df.show()

# Select columns, where filter value
clean_df = df.select('year', 'month', 'PM_Dongsi')\
    .where("PM_Dongsi != 'NA'")

clean_df.show()

# Which month has most pm values
result = clean_df.groupBy('month')\
    .count()\
    .orderBy('count')

result.show()