-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,                                    
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING,
fp1_date DATE,
fp1_time STRING,
fp2_date DATE,
fp2_time STRING,
fp3_date DATE,
fp3_time STRING,
quali_date DATE,
quali_time STRING,
sprint_date DATE,
sprint_time STRING
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/races.csv", header true)


-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/constructors.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId INT,
driverRef STRING,
number INT,
code STRING,
forename STRING,
surname STRING,
dob DATE,
nationality STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/drivers.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/results.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit_stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(raceId INT,
driverId INT,
stop INT,
number INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/pit_stops.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create lap_times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId INT,
driverId INT,
stop INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/lap_times.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING csv
OPTIONS (path "/mnt/formula1nahdl/raw/qualifying.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;