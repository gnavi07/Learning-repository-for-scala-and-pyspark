# Databricks notebook source
data_name = [('Mike','J','Lawerence'),
             ('Kite',None,'Joseph'),
             ('Matthew',None,None),
             ('Andrew','Jr',None)
]

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType

# COMMAND ----------

 name_sc = StructType( fields=[
   StructField("first_name", StringType(),True),
   StructField("second_name", StringType(),True),
   StructField("last_name", StringType(),True),
 ])

# COMMAND ----------

df = spark.createDataFrame(data=data_name,schema=name_sc)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import concat,concat_ws

# COMMAND ----------

df_new = df.withColumn('concat_full_name',concat('first_name','second_name','last_name'))\
.withColumn('concat_ws_full_name',concat_ws(' ','first_name','second_name','last_name'))

# COMMAND ----------

display(df_new)