# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC source_dir = "gs://binumathewtest/clickstream_json"
# MAGIC jsonSchema = StructType([
# MAGIC     StructField("user_id", StringType(), True),StructField("http_vhost_name", StringType(), True),StructField("http_client_ip", StringType(), True),StructField("event_time", StringType(), True), StructField("page", StringType(), True),StructField("user", StringType(), True), StructField("session", StringType(), True) ])
# MAGIC 
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC   .option("cloudFiles.fetchParallelism","4").option("cloudFiles.format","json") \
# MAGIC   .schema(jsonSchema) \
# MAGIC   .load(source_dir) \
# MAGIC   .select(col("user_id").alias("user_id"),get_json_object('session', '$.id').alias('session_id')   
# MAGIC     ,col("http_vhost_name").alias("domain_name"),col("http_client_ip").alias("client_ip") 
# MAGIC     ,regexp_replace(regexp_replace(col("event_time"),'T',' '),'Z','').alias("event_time"),get_json_object('page', '$.country').alias('country')
# MAGIC     ,get_json_object('page', '$.url').alias('page_url'),get_json_object('user', '$.browser').alias('browser')
# MAGIC     ,get_json_object('user', '$.os').alias('os'),get_json_object('user', '$.platform').alias('platform')) \
# MAGIC     .where("country IN ('MX','TW','PE','CO','UY','PR','CL','ID','CA','GB','JP','US','PA')") \
# MAGIC 
# MAGIC df.createOrReplaceTempView("clickstream_json")
# MAGIC display(df.limit(5))
# MAGIC df.writeStream.format("delta").option("checkpointLocation","/tmp/bmathew/demo/clickstream_auto_loader/checkpoint").start("/tmp/bmathew/demo/clickstream_auto_loader/data")
