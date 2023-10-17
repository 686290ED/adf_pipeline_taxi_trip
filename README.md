#ADF pipelines
Functions deployed using ADF pipelines to perform similar operations in [taxi trip analysis dbt project](https://github.com/686290ED/taxi_trip/tree/main)

Using [bash script]() to create a resource group, storage account, storage container and data factory. 
## 1. Copy data from bigquery to blob storage using Copy data operation.
Using [bash script]() to create a link service for bigquery dataset and the blob with properties defined in [bigquery json file]()
and [blob json file](). Using [sql script]() to calculate coordinate range of the area. Using [python script]() to define the datasets 
and the pipeline and run the pipeline.

## 2. Split data monthly using Data flow.
Using [python file]() to define the data flow and pipeline and run the pipeline.

## 3. Calculate nearest weather station to each pickup/dropoff location using Databricks.
Using [bash script]() to create a databricks workspace, get the access key of the blob and save the key in a key vault for databricks keyvault mapping.
Create a databricks notebook using [script]() and define and run the pipeline of the databricks activity using [python script]().

## 4. Join taxi trip data with weather by python script using Batch custom.
Using [bash script]() to create a batch account and a service principal with contributor role to login programmatically.
Create a pipeline with a batch custom activity using [python script]() and the pipeline will run [python script]().