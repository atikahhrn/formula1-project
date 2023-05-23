# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", StringType(), True),
                                      StructField("statusId", IntegerType(), True)
                                     ])

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.schema(results_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/results.csv")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. name added with concatenation of forename and surname
# MAGIC 4. ingestion date added

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

result_with_ingestion_date = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns from the dataframe
# MAGIC 1. drop statusId

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = result_with_ingestion_date.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to processed container in parquet format

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.results
# MAGIC WHERE file_date = '2023-04-02'

# COMMAND ----------

