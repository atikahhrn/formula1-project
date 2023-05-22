# Formula 1 Racing Data Pipeline using Azure
This is a project on building a Cloud Data platform on Azure for reporting and doing analysis of the data from Formula 1 Motorsport.

## Data Source
- The data for this project is obtained from an open source API called [Ergast Developer API](http://ergast.com/mrd/). The API provides data for the Formula One series, from the beginning of the world championships in 1950.
- This project is using the [database tables in CSV format](http://ergast.com/mrd/db/#csv).
- The structure of the database is shown in Entity Relationship Diagram as below and explained in the [Database User Guide](http://ergast.com/docs/f1db_user_guide.txt).

![Entity Relationship Diagram](https://github.com/atikahhrn/formula1-project/assets/108443483/dcd75408-b56f-493a-8fb3-bca9e8125220)

## Architecture Diagram

![Architecture Diagram](https://github.com/atikahhrn/formula1-project/assets/108443483/b024fcb9-5958-41a2-b40f-028fb8dfaabc)

- The Formula 1 database in CSV format is imported from the Ergast API website manually into the Data Lake raw container.
- The data is processed using Databricks Notebooks to ingest into the ingested or the processed layer. The data in this layer will have the schema applied as well as stored in the columnar format parquet. Create partitions where applicable, as well as add additional information for audit purposes such as ingested date, source of the data, etc.
- Ingested data will then be transformed via Azure Databricks notebooks again, and the results are stored in the presentation layer. 
- Databricks notebooks will be used to analyze the data from the presentation layer and dashboards are created to satisfy the requirements for analysis.
- Then, all the notebooks is converted to produce data in Delta Lake to satisfy requirements around GDPR, time travel, and more.
- Azure Data Factory is used as scheduling and monitoring tool.

## Services Used
- Azure Databricks
- Azure Data Lake Storage Gen2
- Azure Data Factory
- PySpark and Spark SQL
- Delta Lake
- Azure Key Vault

## Results
### Dominant Formula 1 Drivers
![Dominant Drivers](https://github.com/atikahhrn/formula1-project/assets/108443483/4afc639a-a096-4720-a64c-7f90c30564cf)

### Dominant Formula 1 Teams
![Dominant Teams](https://github.com/atikahhrn/formula1-project/assets/108443483/575e388c-55a5-4994-9075-26447ea9fbcd)

## Future Works
- Visualize Formula 1 data to identify trends and gain insights in Power BI.
- Using Azure Data Factory Pipeline to automate data fetching process at regular intervals directly from the Ergast API.
