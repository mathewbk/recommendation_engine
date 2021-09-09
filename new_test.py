# Databricks notebook source

   .select(col("user_id").alias("user_id"),get_json_object('session', '$.id').alias('session_id')   
     ,col("http_vhost_name").alias("domain_name"),col("http_client_ip").alias("client_ip") 
    ,regexp_replace(regexp_replace(col("event_time"),'T',' '),'Z','').alias("event_time"),get_json_object('page', '$.country').alias('country')
    ,get_json_object('page', '$.url').alias('page_url'),get_json_object('user', '$.browser').alias('browser')
     ,get_json_object('user', '$.os').alias('os'),get_json_object('user', '$.platform').alias('platform')) \
     .where("country IN ('MX','TW','PE','CO','UY','PR','CL','ID','CA','GB','JP','US','PA')") \

df.createOrReplaceTempView("clickstream_json")
 display(df.limit(5))
df.writeStream.format("delta").option("checkpointLocation","/tmp/bmathew/demo/clickstream_auto_loader/checkpoint").start("/tmp/bmathew/demo/clickstream_auto_loader/data")

