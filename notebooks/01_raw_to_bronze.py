# Databricks notebook source
# MAGIC %md
# MAGIC Check the list of files available from source 

# COMMAND ----------

# MAGIC %fs ls /Volumes/cyber-etl-pipeline/default/inbound/

# COMMAND ----------

# MAGIC %md
# MAGIC Read the source data 

# COMMAND ----------

df = spark.read.option("header", True).csv("/Volumes/cyber-etl-pipeline/default/inbound/cyber_logs/cyber_logs_sample.csv")
df.printSchema()
display(df.show(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Check the record count

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Copy the source data as is to Silver (Ingestion)

# COMMAND ----------

df = spark.read.option("header", True).csv("/Volumes/cyber-etl-pipeline/default/inbound/cyber_logs/cyber_logs_sample.csv")
df.printSchema()
display(df.show(5))
df_clean = df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Write Lightly cleaned data to Silver layer

# COMMAND ----------

# Save as Delta format 
df_clean.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/Volumes/cyber-etl-pipeline/default/outbound/bronze/cyber_logs/")


# COMMAND ----------

df_clean.count()

# COMMAND ----------

df_clean.show(10)

# COMMAND ----------

from pyspark.sql.functions import col
df_clean.filter(col("label") == 0).show()

# COMMAND ----------


