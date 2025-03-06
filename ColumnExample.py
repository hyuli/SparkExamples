from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType

spark = SparkSession.builder\
    .appName('DataTypeExample')\
    .master('local')\
    .getOrCreate()

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("First", StringType(), True),
    StructField("Last", StringType(), True),
    StructField("Url", StringType(), True),
    StructField("Published", StringType(), True),
    StructField("Hits", LongType(), True),
    StructField("Campaigns", ArrayType(StringType()), True),
])

df = spark.read\
    .schema(schema)\
    .json('dataset/blogs.txt')


# 1) get column
# - by df
print(df['First'])
# - by col better
print(col('First'))

# 2) select by columns
df.select(col('First'), col('Last'), col('Hits')).show()

# 3) double the hits
df.select(col('First'), col('Last'), col('Hits'), col('Hits') * 2).show()

# 4) order by
df.orderBy(col('Hits').desc()).show()

# 5) True or false
df.select(col('First'), col('Last'), col('Hits'), col('Hits') > 10000).show()

# 6) search string
df.where(col('First') == 'Denny').show()

# 7) short way.
# No operations on this columns, replace it with string, without col()
df.select('First', 'Last', 'Hits').show()

df.select('First', 'Last', 'Hits', col('Hits') * 2).show()

# 8) drop a column
df.drop('Hits').show()

# 9) create a column
df.withColumn('FullName', concat('First',lit(' '), 'Last')).show()
# Get column by two days:
# 1) col('') to get column object
# 2) string to get column(column with string name must exits)

#10) rename of column
df.withColumnRenamed('First', 'FirstName').show()