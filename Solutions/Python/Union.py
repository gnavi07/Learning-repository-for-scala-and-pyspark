from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4040')\
        .getOrCreate()

df0 = spark.createDataFrame([('Pat', 'Dev',),('joseph','Prod'),('Raj','Test')], ['Name', 'Dept'])
df1 = spark.createDataFrame([('Dev', 'Taligsh',),('Prod','Washim'),], ['Dept', 'Name'])

df_union = df0.union(df1)
# df_union.show()
# +------+-------+
# |  Name|   Dept|
# +------+-------+
# |   Pat|    Dev|
# |joseph|   Prod|
# |   Raj|   Test|
# |   Dev|Taligsh|
# |  Prod| Washim|
# +------+-------+

df_union_name = df0.unionByName(df1,allowMissingColumns=True)

# df_union_name.show()
# +-------+----+
# |   Name|Dept|
# +-------+----+
# |    Pat| Dev|
# | joseph|Prod|
# |    Raj|Test|
# |Taligsh| Dev|
# | Washim|Prod|
# +-------+----+
