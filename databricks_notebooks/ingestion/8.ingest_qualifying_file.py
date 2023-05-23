# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read \
.option("header", True) \
.schema(qualifying_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/qualifying.csv")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyId, raceId, driverId and constructorId
# MAGIC 2. Add ingestion date with current timestamp

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

