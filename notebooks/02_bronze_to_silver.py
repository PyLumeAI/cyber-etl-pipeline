# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read from Bronze

# COMMAND ----------

df_bronze = spark.read.format("delta").load("dbfs:/Volumes/cyber-etl-pipeline/default/outbound/bronze/cyber_logs/") 

# COMMAND ----------

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Rename the columns

# COMMAND ----------

# Create a mapping dictionary
column_mapping = {
    "srcip": "source_ip",
    "sport": "source_port",
    "dstip": "destincation_ip",
    "dsport":"destincation_port",
    "proto":"transaction_protocol",
    "state":"state",
    "dur":"record_total_duration",
    "sbytes":"src_dst_bytes",
    "dbytes":"dst_src_bytes",
    "attack_cat":"attack_category",
    "label":"attack_label"

}

# Apply the renaming
for old_name, new_name in column_mapping.items():
    df_bronze = df_bronze.withColumnRenamed(old_name, new_name)

print(df_bronze.columns)

# COMMAND ----------

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Cast data types

# COMMAND ----------

#Convert attack_label column to boolean, It will have only 0 or 1 value

from pyspark.sql.functions import col, when

df_casted = df_bronze.select(
  col("record_total_duration").cast("float").alias("record_total_duration"),
  col("src_dst_bytes").cast("long").alias("src_dst_bytes"),
  col("dst_src_bytes").cast("long").alias("dst_src_bytes"),
  when (col("attack_label") == '1', True).otherwise(False).alias("attack_label")
)

# COMMAND ----------

df_casted.show(5)

# COMMAND ----------

df_casted.filter("attack_label = true").show(10)
df_casted.filter("attack_label = false").show(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Write enriched data to silver

# COMMAND ----------

df_casted.write.format("delta").mode("overwrite").save("dbfs:/Volumes/cyber-etl-pipeline/default/outbound/silver/cyber_logs/")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the record count

# COMMAND ----------

df_casted.count()
