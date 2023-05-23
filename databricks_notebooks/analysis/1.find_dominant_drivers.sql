-- Databricks notebook source
SELECT driver_name,
       COUNT(1) AS total_races, --to see how many races so that we not only see based on total_points
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50 --Do this because if a person race one time but get high point then his average point will be high, look at more races
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2023
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

