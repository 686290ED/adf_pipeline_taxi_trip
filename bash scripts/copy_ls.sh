export resourceGroup=rg_adf
export location=westus2
export storageAccount=storagetaxi0001 # only letters and numbers allowed in name
export storageContainer=storagecontainer-taxi
export dataFactory=adf-taxi-trip-0023
# linked service
export linkedServiceBigQuery=lsbigquery
export linkedServiceBlob=lsblob


# Create a linked service and datasets.
# Create a json file for connection information.
# reference https://learn.microsoft.com/en-us/azure/data-factory/connector-google-bigquery?tabs=data-factory

# Create a linked service for Bigquery.
az datafactory linked-service create --resource-group $resourceGroup \
    --factory-name $dataFactory --linked-service-name $linkedServiceBigQuery \
    --properties /home/guo/adf_taxi_trips/adf_linked_service_bigquery.json

# Create a linked service for the blob.
azureConnectionString="${azureConnectionString//\//\\\/}"
sed -i "s/.*connectionString.*/\ \ \ \ \"connectionString\": \"${azureConnectionString}\"/" /home/guo/adf_taxi_trips/adf_linked_service_blob.json
az datafactory linked-service create --resource-group $resourceGroup \
    --factory-name $dataFactory --linked-service-name $linkedServiceBlob \
    --properties /home/guo/adf_taxi_trips/adf_linked_service_blob.json