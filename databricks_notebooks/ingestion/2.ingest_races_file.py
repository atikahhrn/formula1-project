# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2023-02-26")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

display(spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# Specify schema

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),                                    
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True),
                                   StructField("fp1_date", DateType(), True),
                                   StructField("fp1_time", StringType(), True),
                                   StructField("fp2_date", DateType(), True),
                                   StructField("fp2_time", StringType(), True),
                                   StructField("fp3_date", DateType(), True),
                                   StructField("fp3_time", StringType(), True),
                                   StructField("quali_date", DateType(), True),
                                   StructField("quali_time", StringType(), True),
                                   StructField("sprint_date", DateType(), True),
                                   StructField("sprint_time", StringType(), True)
                                                                   
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add race_timestamp and ingestion_date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_with_ingestion_date = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns & rename as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_final_df = races_with_ingestion_date.select(col("raceId").alias("race_id"), col("year").alias("race_year"),
                                                  col("round"), col("circuitId").alias("circuit_id"), col("name"),
                                                  col("ingestion_date"), col("race_timestamp")) \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the output to processed container in parquet format

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

