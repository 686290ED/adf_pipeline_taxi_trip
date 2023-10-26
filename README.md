# ADF pipelines
Functions deployed using ADF pipelines to perform similar operations in 
[taxi trip analysis dbt project](https://github.com/686290ED/taxi_trip/tree/main).

[Bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/init.sh) creates a resource group,
 storage account, storage container and data factory. 
## 1. Copy data from BigQuery to blob storage using Copy data operation.
[Bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/copy_ls.sh) creates a link service for connecting to BigQuery
and blob storage with properties defined in [BigQuery JSON file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_linked_service_bigquery.JSON)
and [blob JSON file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_linked_service_blob.JSON).
[SQL script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/get_coordinate_range.sql) calculates coordinate range of the area.  
[Python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_pipeline_copy.py) defines datasets 
and the pipeline and runs the pipeline.

## 2. Split data monthly using Data flow.
[Python file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/split%20data/adf_pipeline_split.py) defines the data flow and pipeline and runs the pipeline.

## 3. Calculate nearest weather station to each pickup/drop-off location using Databricks.
[Bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/Databricks.sh) creates a Databricks workspace, 
gets the storage access key and saves it in a key vault, which will be added to Databricks secret scope.
Create a Databricks notebook using [script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/coordinate_nearest_station/Databricks_python_nearest_station.py). 
Define and run pipeline of the Databricks activity using [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/coordinate_nearest_station/adf_pipeline_nearest_station.py).

## 4. Join taxi trip data with weather by python script using Batch custom.
[Bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/batch.sh) creates a batch account and a service principal with contributor role to login programmatically.
The join operation is defined in [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/join%20data/df_join.py).
[Python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/join%20data/adf_pipeline_join.py) creates a pipeline with a batch custom activity and runs the pipeline.