from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType, Row

spark = SparkSession.builder\
    .appName('FunctionExample')\
    .master('local')\
    .getOrCreate()

# 1) map function, from RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
result = rdd.map(lambda x: x * x)

rdd = spark.sparkContext.parallelize([('tom', 10), ('jerry', 20)])
df = rdd.toDF(['name', 'score'])

# df.select(col('score') * col('score')).show()

# rdd -> flatmap -> write hello world for bigdata
# spark sql -> explode
df = spark.read.text('dataset/LICENSE.txt')
# split to splits word by separator
df.show(truncate=False)

words_df = df.select(split('value', ' ').alias('words'))
words_df.show(truncate=False)

word_df = words_df.select(explode('words').alias('word'))
word_df.show(100, truncate=False)

result = word_df.groupBy('word')\
    .count()\
    .orderBy(col('count').desc())

result.show()

# chaining col function calls
result = df.select(explode(split('value', ' ')).alias('word'))\
    .groupBy('word')\
    .count()\
    .orderBy(col('count').desc())

result.show()


# 3) filter
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18)])
df = rdd.toDF(['name', 'age'])

# where SQL
df.where(col('age') > 18).show()
# filter RDD
df.filter(col('age') > 18).show()

# 4) order by
rdd = spark.sparkContext.parallelize([("jayChou", 41), ("burukeyou", 23)])
df = spark.createDataFrame(rdd.map(lambda row: Row(name=row[0], age=row[1])))

df.orderBy(col('age').asc()).show() # asc: from min to max
df.orderBy(col('age').desc()).show() # desc: from max to min

df.sort(col('age').asc()).show()

# 5) drop duplicates
rdd = spark.sparkContext.parallelize([("jayChou", 41), ("burukeyou", 23), ("burukeyouClone", 23), ("burukeyou", 23)])

df = spark.createDataFrame(rdd.map(lambda row: Row(name=row[0], age=row[1])))

df.dropDuplicates(['name', 'age']).show()

# 6) distinct
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18), ('tom', 21)])
df = rdd.toDF(['name', 'age'])

df.select('name').distinct().show()

# 7) limit
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18), ('tom', 21)])
df = rdd.toDF(['name', 'age'])

df.limit(2).show()

# 8) more sql functions
rdd = spark.sparkContext.parallelize([('tom enn', 20), ('jack zhou', 18), ('tom', 21)])
df = rdd.toDF(['name', 'age'])

df.filter(col('name').like('%tom%')).show()