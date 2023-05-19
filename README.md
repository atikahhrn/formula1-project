# Formula 1 Racing Data Pipeline using Azure

## Problem Statement
Develop a data pipeline to perform analysis and visualization for Formula 1 racing.

## Data Source
- The data for this project is obtained from an open source API called [Ergast Developer API](http://ergast.com/mrd/). The API provides data for the Formula One series, from the beginning of the world championships in 1950.
- This project is using the [database tables in CSV format](http://ergast.com/mrd/db/#csv).
- The structure of the database is shown in Entity Relationship Diagram as below and explained in the [Database User Guide](http://ergast.com/docs/f1db_user_guide.txt).

![Entity Relationship Diagram](https://github.com/atikahhrn/formula1-project/assets/108443483/dcd75408-b56f-493a-8fb3-bca9e8125220)

## Services Used
- Azure Databricks
- Azure Data Lake Storage Gen2
- Azure Data Factory
- PySpark and Spark SQL
- Delta Lake
- Azure Key Vault

## Solutions Architecture

![Solutions Architecture](https://github.com/atikahhrn/formula1-project/assets/108443483/921c4320-d43c-4cd1-891e-06ad1a15432f)
