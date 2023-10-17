#ADF pipelines
Functions deployed using ADF pipelines to perform similar operations in 
[taxi trip analysis dbt project](https://github.com/686290ED/taxi_trip/tree/main).

Using [bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/init.sh) to create a resource group,
 storage account, storage container and data factory. 
## 1. Copy data from bigquery to blob storage using Copy data operation.
Using [bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/copy_ls.sh) to create a link service for bigquery connection
and the blob with properties defined in [bigquery json file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_linked_service_bigquery.json)
and [blob json file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_linked_service_blob.json).
 Using [sql script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/get_coordinate_range.sql) to calculate coordinate range of the area. 
 Using [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/copy%20data/adf_pipeline_copy.py) to define the datasets 
and the pipeline and run the pipeline.

## 2. Split data monthly using Data flow.
Using [python file](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/split%20data/adf_pipeline_split.py) to define the data flow and pipeline and run the pipeline.

## 3. Calculate nearest weather station to each pickup/dropoff location using Databricks.
Using [bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/databricks.sh) to create a databricks workspace, 
get the access key of the blob and save the key in a key vault for databricks keyvault mapping.
Create a databricks notebook using [script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/coordinate_nearest_station/databricks_python_nearest_station.py)
 and define and run the pipeline of the databricks activity using [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/coordinate_nearest_station/adf_pipeline_nearest_station.py).

## 4. Join taxi trip data with weather by python script using Batch custom.
Using [bash script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/bash%20scripts/batch.sh) to create a batch account and a service principal with contributor role to login programmatically.
The operation is defined in [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/join%20data/df_join.py).
Create a pipeline with a batch custom activity and run the pipeline using [python script](https://github.com/686290ED/adf_pipeline_taxi_trip/blob/main/join%20data/adf_pipeline_join.py).
