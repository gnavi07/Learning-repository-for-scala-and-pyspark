# Truth table

# Import SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("truthTable").getOrCreate()

df=spark.read\
.format('csv')\
.option('header',True)\
.load('<inputfile>/truth.csv')

# truth.csv file
# flag0
# 0
# 1

df1=df.withColumnRenamed('flag0','flag1')
df2=df.withColumnRenamed('flag0','flag2')
df3=df.withColumnRenamed('flag0','flag3')
df4=df.withColumnRenamed('flag0','flag4')
df5=df.withColumnRenamed('flag0','flag5')
df6=df.withColumnRenamed('flag0','flag6')
df7=df.withColumnRenamed('flag0','flag7')
df8=df.withColumnRenamed('flag0','flag8')
df9=df.withColumnRenamed('flag0','flag9')

dffinal = df.crossJoin(df1).crossJoin(df2).crossJoin(df3).crossJoin(df4).crossJoin(df5).crossJoin(df6)\
.crossJoin(df7).crossJoin(df8).crossJoin(df9)

dffinal.show()

# +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
# |flag0|flag1|flag2|flag3|flag4|flag5|flag6|flag7|flag8|flag9|
# +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
# |    0|    0|    0|    0|    0|    0|    0|    0|    0|    0|
# |    0|    0|    0|    0|    0|    0|    0|    0|    0|    1|
# |    0|    0|    0|    0|    0|    0|    0|    0|    1|    0|
# |    0|    0|    0|    0|    0|    0|    0|    0|    1|    1|
# |    0|    0|    0|    0|    0|    0|    0|    1|    0|    0|
# |    0|    0|    0|    0|    0|    0|    0|    1|    0|    1|
# |    0|    0|    0|    0|    0|    0|    0|    1|    1|    0|
# |    0|    0|    0|    0|    0|    0|    0|    1|    1|    1|
# |    0|    0|    0|    0|    0|    0|    1|    0|    0|    0|
# |    0|    0|    0|    0|    0|    0|    1|    0|    0|    1|
# |    0|    0|    0|    0|    0|    0|    1|    0|    1|    0|
# |    0|    0|    0|    0|    0|    0|    1|    0|    1|    1|
# |    0|    0|    0|    0|    0|    0|    1|    1|    0|    0|
# |    0|    0|    0|    0|    0|    0|    1|    1|    0|    1|
# |    0|    0|    0|    0|    0|    0|    1|    1|    1|    0|
# |    0|    0|    0|    0|    0|    0|    1|    1|    1|    1|
# |    0|    0|    0|    0|    0|    1|    0|    0|    0|    0|
# |    0|    0|    0|    0|    0|    1|    0|    0|    0|    1|
# |    0|    0|    0|    0|    0|    1|    0|    0|    1|    0|
# |    0|    0|    0|    0|    0|    1|    0|    0|    1|    1|
# +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
# only showing top 20 rows


dffinal.count()

# 1024

spark.stop()
