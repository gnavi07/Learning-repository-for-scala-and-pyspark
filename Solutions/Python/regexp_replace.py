%run ./Includes/Classroom-Setup

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
destFile = workingDir + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

emp_df = spark.read.format('csv')\
.option('sep',':')\
.option('header',True)\
.load(sourceFile)


from pyspark.sql.functions import initcap,regexp_replace
emp_df = emp_df.withColumn('firstName',initcap('firstName'))\
.withColumn('middleName',initcap('middleName'))\
.withColumn('lastName',initcap('lastName'))\
.withColumn('gender',initcap('gender'))

from pyspark.sql.functions import regexp_replace
emp_df = emp_df.withColumn('ssn', regexp_replace('ssn', '-', ''))

emp_df.show(15)
# +---------+----------+-----------+------+----------+------+---------+
# |firstName|middleName|   lastName|gender| birthDate|salary|      ssn|
# +---------+----------+-----------+------+----------+------+---------+
# |  Randall|       Jim|   Carrauza|     M|1925-01-26| 52458|902233313|
# |    Toney|     Allen|     Donaho|     M|1996-07-03| 27162|943487835|
# |   Margie|    Debera|      Sylve|     F|1983-09-13|159856|983331570|
# |   Carlos|    Anibal|      Dajer|     M|1978-06-16|127009|903874355|
# |   Oralia|      Jana|    Holland|     F|2013-01-09|196957|955905337|
# | Latashia|   Romaine|    Mendell|     F|1937-03-07| 26045|995172519|
# |  Herbert|   Francis|      Dubre|     M|1953-06-25|244905|941432907|
# |   Idella|   Kristle|    Holyoke|     F|1923-07-02| 68740|905209296|
# |     Leon|   Quentin|Vannorsdell|     M|1988-11-11| 67097|993773462|
# |   Danial|     Britt|   Bakowski|     M|1939-02-20|171463|934316034|
# |Georgette|     Verda|      Thill|     F|1915-09-27|182628|908961259|
# |   Claude|      Earl|      Strek|     M|1943-08-21| 76434|906407196|
# |      Ken|    Adolph|    Rickett|     M|1936-03-16|138064|983111228|
# |     Dena|     Velma|      Terzo|     F|1954-01-16|102840|906463057|
# |     Kori|    Cristi|     Felson|     F|1949-05-10|248912|952845058|
# +---------+----------+-----------+------+----------+------+---------+

emp_df = emp_df.dropDuplicates()
emp_df= emp_df.repartition(8)
emp_df.write.format('delta').mode('overwrite').save(destFile)

partFiles = len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls(destFile))))

finalDF = spark.read.format('delta').load(destFile)
finalCount = finalDF.count()

assert partFiles == 8, "expected 8 parquet files located in destFile"
assert finalCount == 100000, "expected 100000 records in finalDF"

%run "./Includes/Classroom-Cleanup"
