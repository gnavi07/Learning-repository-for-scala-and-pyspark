# Import SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("errormsg").getOrCreate()

df=spark.read\
.format('csv')\
.option('header',True)\
.option("TreatEmptyValuesAsNulls", True)\
.option("IgnoreLeadingWhiteSpace", True)\
.option("IgnoreTrailingWhiteSpace", True)\
.load('<filepath>/errormsg.csv')

from pyspark.sql.functions import *

# App Error
appDF = df.drop('WebError')
appDF = appDF.withColumnRenamed('AppError','Error')

# Web Error
webDF = df.drop('AppError')
webDF = webDF.withColumnRenamed('WebError','Error')

#Union two DF
finalDF=appDF.union(webDF)

finalDF.orderBy('Application').show()

# +-----------+--------------------+
# |Application|               Error|
# +-----------+--------------------+
# |   BMI Calc|     Precision Error|
# |   BMI Calc|          Page Error|
# | BloodGroup|      Pageload Error|
# | BloodGroup|         Login Error|
# |SugarFinder|          Auth Error|
# |SugarFinder|Result dispaly error|
# +-----------+--------------------+

spark.stop()
