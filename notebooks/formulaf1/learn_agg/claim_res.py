# Databricks notebook source
# MAGIC %run "../includes/config_file_path"

# COMMAND ----------

claim_df = spark.read\
.option('header',True)\
.option('inferSchema',True)\
.csv(f'{raw_folder_path}/claim_data.csv')

# COMMAND ----------

display(claim_df)

# COMMAND ----------

from pyspark.sql.functions import sum,year,to_date

# COMMAND ----------

transaction_df = claim_df.groupBy('Client_Name')\
.agg(sum('Amount_claimed').alias('total_claimed'),sum('Amount_dispersed').alias('total_dispersed'))

# COMMAND ----------

transaction_df = transaction_df.withColumn('Amount_Not_cleared',(transaction_df['total_claimed']-transaction_df['total_dispersed'])) \
.select('Client_Name','total_claimed','total_dispersed','Amount_Not_cleared')

# COMMAND ----------

display(transaction_df)

# COMMAND ----------

ledger_df = claim_df.select(year(to_date(claim_df.Claim_date,'dd-MM-yyyy')).alias('Year'),'Amount_claimed','Amount_dispersed')


# COMMAND ----------

display(ledger_df)

# COMMAND ----------

ledger_df = ledger_df.groupBy('Year')\
.agg(sum('Amount_claimed').alias('total_claimed'),sum('Amount_dispersed').alias('total_dispersed'))

# COMMAND ----------

ledger_df = ledger_df.withColumn('Amount_Not_cleared',(ledger_df['total_claimed']-ledger_df['total_dispersed'])) \
.select('Year','total_claimed','total_dispersed','Amount_Not_cleared')

# COMMAND ----------

display(ledger_df)

# COMMAND ----------

