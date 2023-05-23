# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("forename", StringType(), True),
                                      StructField("surname", StringType(), True),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

drivers_df = spark.read \
.option("header", True) \
.schema(drivers_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/drivers.csv")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. name added with concatenation of forename and surname
# MAGIC 4. ingestion date added

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("forename"), lit(" "), col("surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("data_source", lit(v_file_date))

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns from the dataframe
# MAGIC 1. drop forename, surname and url

# COMMAND ----------

drivers_final_df = drivers_with_ingestion_date_df.drop('forename', 'surname', 'url')

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

