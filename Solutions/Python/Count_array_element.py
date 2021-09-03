#Array
# Import SparkSession

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Array").getOrCreate()

from pyspark.sql.functions import split,regexp_replace,expr

df=spark.read\
.format('csv')\
.option('header',True)\
.option('sep','|')\
.load('<inpfile>/array_data.csv')

# df.show()
# +---+--------------------+
# | ID|                Code|
# +---+--------------------+
# |  1|["A1","B1","C1","...|
# |  2|         ["A1","A1"]|
# |  3|         ["B1","C1"]|
# |  4|              ["C1"]|
# +---+--------------------+

# df.printSchema()
# root
#  |-- ID: string (nullable = true)
#  |-- Code: string (nullable = true)

df1=df.withColumn('Code',split('Code','\[')[1])\
      .withColumn('Code',split('Code','\]')[0])\
      .withColumn('Code',regexp_replace('Code', '\"', ''))\
      .withColumn('CodeAr', split('Code', '\,')) \
      .withColumn('A1cnt', expr('size(filter(CodeAr, x -> x in ("A1")))'))\
      .withColumn('B1cnt', expr('size(filter(CodeAr, x -> x in ("B1")))'))\
      .withColumn('C1cnt', expr('size(filter(CodeAr, x -> x in ("C1")))'))\

# df1.show()
# +---+-----------+----------------+-----+-----+-----+
# | ID|       Code|          CodeAr|A1cnt|B1cnt|C1cnt|
# +---+-----------+----------------+-----+-----+-----+
# |  1|A1,B1,C1,A1|[A1, B1, C1, A1]|    2|    1|    1|
# |  2|      A1,A1|        [A1, A1]|    2|    0|    0|
# |  3|      B1,C1|        [B1, C1]|    0|    1|    1|
# |  4|         C1|            [C1]|    0|    0|    1|
# +---+-----------+----------------+-----+-----+-----+

# df1.printSchema()
# root
#  |-- ID: string (nullable = true)
#  |-- Code: string (nullable = true)
#  |-- CodeAr: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- A1cnt: integer (nullable = false)
#  |-- B1cnt: integer (nullable = false)
#  |-- C1cnt: integer (nullable = false)

df1=df1.drop('CodeAr')
# df1.show()
# +---+-----------+-----+-----+-----+
# | ID|       Code|A1cnt|B1cnt|C1cnt|
# +---+-----------+-----+-----+-----+
# |  1|A1,B1,C1,A1|    2|    1|    1|
# |  2|      A1,A1|    2|    0|    0|
# |  3|      B1,C1|    0|    1|    1|
# |  4|         C1|    0|    0|    1|
# +---+-----------+-----+-----+-----+

spark.stop()
