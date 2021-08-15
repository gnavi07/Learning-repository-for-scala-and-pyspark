# Import SparkSession

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pySpark").getOrCreate()

df=spark.read\
.format('csv')\
.option('header',True)\
.option("TreatEmptyValuesAsNulls", True)\
.option("IgnoreLeadingWhiteSpace", True)\
.option("IgnoreTrailingWhiteSpace", True)\
.load('<inputfilepath>/employee_access.csv')

from pyspark.sql.functions import *

# Filter Dataframe
devDF = df.filter(df['Devlopment'] == 'Y')
intDF = df.filter(df['Integration'] == 'Y') 
rlsDF = df.filter(df['Release'] == 'Y') 
prdDF = df.filter(df['Production'] == 'Y') 

devDF = devDF.withColumn('Access',lit('Development'))
intDF = intDF.withColumn('Access',lit('Integration'))
rlsDF = rlsDF.withColumn('Access',lit('Release'))
prdDF = prdDF.withColumn('Access',lit('Production'))

finalDF = devDF.union(intDF).union(rlsDF).union(prdDF)

finalDF.orderBy('EmployeeID').show()

# +----------+----------+-----------+-------+----------+-----------+
# |EmployeeID|Devlopment|Integration|Release|Production|     Access|
# +----------+----------+-----------+-------+----------+-----------+
# |      1123|         Y|          Y|      N|         N|Integration|
# |      1123|         Y|          Y|      N|         N|Development|
# |      1125|         N|          N|      N|         Y| Production|
# |      1235|         Y|          Y|      Y|         N|    Release|
# |      1235|         Y|          Y|      Y|         N|Development|
# |      1235|         Y|          Y|      Y|         N|Integration|
# |      2225|         N|          Y|      N|         N|Integration|
# |      5891|         Y|          N|      N|         N|Development|
# |      5898|         N|          N|      Y|         N|    Release|
# |      7890|         N|          N|      N|         Y| Production|
# +----------+----------+-----------+-------+----------+-----------+

accessDF = finalDF.select('EmployeeID','Access')

accessDF.show()

# +----------+-----------+
# |EmployeeID|     Access|
# +----------+-----------+
# |      1123|Development|
# |      1235|Development|
# |      5891|Development|
# |      1123|Integration|
# |      1235|Integration|
# |      2225|Integration|
# |      1235|    Release|
# |      5898|    Release|
# |      1125| Production|
# |      7890| Production|
# +----------+-----------+

spark.stop()

