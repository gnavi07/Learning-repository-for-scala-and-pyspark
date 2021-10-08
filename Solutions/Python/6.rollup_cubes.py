# Databricks notebook source
# MAGIC %md
# MAGIC ### Rollup, Cubes and Grouping id
# MAGIC Spark Multidimensional aggregation
# MAGIC 
# MAGIC 1. Rollup with 2 Columns
# MAGIC 2. Cube with 2 Columns
# MAGIC 3. Rollup with 3 Columns
# MAGIC 4. Cube with 3 Columns

# COMMAND ----------

# MAGIC %md
# MAGIC Input dataset is retail_store details
# MAGIC 
# MAGIC |Date|Outlet|Location|Sales_Amount|
# MAGIC |----|------|--------|------------|
# MAGIC |2021-01-01|Big G Basket|Chennai|14500.5|
# MAGIC |2021-01-01|Rio D Mall|Chennai|158000.95|
# MAGIC |2021-01-01|Phonexinsk Nsk|Chennai|855558.85|
# MAGIC |2021-01-01|Yashik Maskaj|Chennai|88555.48|
# MAGIC |2021-01-01|Big G Basket|Madurai|14500.5|
# MAGIC |2021-01-01|Rio D Mall|Madurai|198000.95|
# MAGIC |2021-01-01|Phonexinsk Nsk|Madurai|85558.85|
# MAGIC |2021-01-01|Yashik Maskaj|Madurai|89555.48|
# MAGIC |2021-01-02|Big G Basket|Chennai|14700.5|
# MAGIC |2021-01-02|Rio D Mall|Chennai|158500.95|
# MAGIC |2021-01-02|Phonexinsk Nsk|Chennai|85558.85|
# MAGIC |2021-01-02|Yashik Maskaj|Chennai|89555.48|
# MAGIC |2021-01-02|Big G Basket|Madurai|148800.5|
# MAGIC |2021-01-02|Rio D Mall|Madurai|158008.95|
# MAGIC |2021-01-02|Phonexinsk Nsk|Madurai|855558.85|
# MAGIC |2021-01-02|Yashik Maskaj|Madurai|89545.48|

# COMMAND ----------

retail_db = spark.read\
.format('csv')\
.option('header',True)\
.option('inferSchema',True)\
.load(f'{raw_folder_path}/retail_store.csv')

# COMMAND ----------

from pyspark.sql.functions import sum,grouping_id

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Grouping ID
# MAGIC 1. 0 -> Group by Outlet, location
# MAGIC 2. 1 -> Group by Individual Outlet
# MAGIC 3. 2 -> Group by Individual location
# MAGIC 4. 3 -> Complete aggreation of the result df

# COMMAND ----------

rollupres = retail_db.rollup('Outlet','Location')\
.agg(sum('Sales_Amount').alias('Total_Sales'),grouping_id().alias('gid'))\
.orderBy('Outlet','Location')

# COMMAND ----------

display(rollupres)

# COMMAND ----------

cuberes = retail_db.cube('Outlet','Location')\
.agg(sum('Sales_Amount').alias('total_sales'),grouping_id().alias('gid'))\
.orderBy('Outlet','Location')

# COMMAND ----------

display(cuberes)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Grouping ID
# MAGIC 1. 0 -> Group by Date, Outlet, location
# MAGIC 2. 1 -> Group by Date+Outlet
# MAGIC 3. 2 -> Group by Date+location
# MAGIC 4. 3 -> Group by Date
# MAGIC 5. 4 -> Group by Outlet+location
# MAGIC 6. 5 -> Group by Outlet
# MAGIC 7. 6 -> Group by location
# MAGIC 8. 7 -> Complete aggreation of result df

# COMMAND ----------

rollupres = retail_db.rollup('Date','Outlet','Location')\
.agg(sum('Sales_Amount').alias('total_sales'),grouping_id().alias('gid'))\
.orderBy('Date','Outlet','Location')

# COMMAND ----------

display(rollupres)

# COMMAND ----------

cuberes1 = retail_db.cube('Date','Outlet','Location')\
.agg(sum('Sales_Amount').alias('total_sales'),grouping_id().alias('gid'))\
.orderBy('Date','Outlet','Location')

# COMMAND ----------

display(cuberes1)

# COMMAND ----------

# MAGIC %md
# MAGIC Maximum group id determined by this formula
# MAGIC 2^n - 1  -> where 'n' is the number of columns used for grouping
