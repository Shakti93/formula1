# ETL Data Engineering Pipeline for Formula 1 Data using Azure Platform

## Introduction
Formula 1 is a popular motorsport that generates a vast amount of data. This data can be used to gain insights into the performance of drivers, teams, and cars. With the advent of cloud computing and big data technologies, it has become easier to process and analyze this data to gain valuable insights. The objective of this project is to build an ETL Data Engineering Pipeline for Formula 1 Data using Azure Platform by ingesting raw data from external sources to Azure Data Lake Storage (ADLS) using Azure Data Factory, processing and transforming the data using Azure Databricks and creating visualizations and reports using Power BI.


## Solution Architecture

![F1 Solution Diagram](https://github.com/Shakti93/formula1-project/assets/84408451/73490a10-4d57-4908-8169-fa901a77ddab)



## The ETL pipeline consists of the following components: 
1. **Data Sources:** The data source for this pipeline is the SQL Server and Formula1 GitHub repository. The data sources contain files for races, results, constructors, drivers, circuits, lap times, pitstops, and qualifying data from the year 1950 to the latest race of 2023. 
2. **Data Extraction:** The data is extracted from SQL Server and Github Repository using Azure Data Factory to Azure Data Lake Storage and stored in the raw container. 
3. **Data Transformation:** The raw data is transformed using Azure Databricks. The transformation process includes data cleaning, data validation, and data enrichment. The transformed data is stored in ADLS. 
4. **Incremental Load using Delta Tables:** Race, Results, Qualifying, and Pitstops data is extracted race-wise (race_id) and incrementally loaded into delta tables to ensure that only new and updated data is loaded into ADLS.  
5. **Data Storage:** The data is stored in ADLS, which is a scalable and cost-effective storage solution for big data analytics workloads. Delta tables are used to store the transformed data, providing transactional capabilities, versioning, and schema enforcement 
6. **Data Visualization:** The data is visualized using Power BI. The visualizations include charts and graphs that show the Race results, performance of Drivers and Teams for the current season, and dominating drivers and teams of history. 


## Implementation 
The ETL pipeline was implemented using the following steps: 

1. **Data Ingestion:** The first pipeline extracts the files for a race ID from the SQL Server and stores them in an ADLS raw container in a race ID folder structure (Bronze layer). 
2. **Data Processing:** The second pipeline executes the Databricks Processing Job. The job has 8 notebooks that read the individual raw files, clean and transform the data, and load the data into the processed database as delta tables using Upsert functionality (Silver layer).  
3. **Data Transformation:** The third pipeline executes the Databricks Transformation Job. The processed data in Delta tables is joined and aggregated for analysis and visualization. The aggregated data is stored in the presentation database as delta tables. The presentation tables are – Race Results, Driver Standings, and Constructor Standings. 
4. **Data Visualization:** The data is visualized using Power BI. The visualizations included charts, graphs, and tables that provided insights into the performance of teams and drivers. 


>
> The pipeline is scheduled to run on a weekly basis, specifically on Mondays. However, given that Formula 1 races do not occur every week, a Look activity is incorporated within the master pipeline. The purpose of these activities is to assess whether a race is scheduled for a given week. If a race is indeed scheduled, the race ID is then passed as a parameter to all three pipelines through the ForEach activity. This approach ensures that our pipelines are optimized for efficiency and only run when necessary. 
>


## Visualizations 
The visualizations for the formula1 data include: 

1. **Race Results:** The race results visualization provides information on the position of drivers in each race, the number of laps completed, and the time taken to complete the race. 
2. **Driver Standings:** The driver standings visualization provides information on the position of drivers in the championship standings, the number of points earned, and the number of podium finishes. 
3. **Team Standings:** The team standings visualization provides information on the position of teams in the championship standings, the number of points earned, and the number of podium finishes. 
4. **Driver Performance:** The Driver Performance provides information on the performance of top drivers over time, including points scored, wins, and podium finishes. 
5. **Team Performance:** The Team Performance provides information on the performance of top constructors over time, including points scored, wins, and podium finishes. 


## Conclusion 
In conclusion, the ETL Data Engineering Pipeline for Formula 1 Data using Azure Platform is a powerful solution that enables the processing and analysis of vast amounts of data generated by Formula 1 races. The pipeline uses Azure Data Factory, Azure Databricks, and Power BI to extract, transform, and visualize the data, providing valuable insights into the performance of drivers, teams, and cars. The pipeline is designed to run on a weekly basis, ensuring that it is optimized for efficiency and only runs when necessary. The visualizations provided by the pipeline include race results, driver and team standings, and driver and team performance over time. This pipeline is an excellent example of how cloud computing and big data technologies can be used to gain valuable insights into complex datasets, and it is a testament to the power of Azure Platform. 
