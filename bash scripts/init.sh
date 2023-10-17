# reference https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-azure-cli
# reference https://learn.microsoft.com/en-us/azure/data-factory/how-to-expression-language-functions
export resourceGroup=rg_adf
export location=westus2
export storageAccount=storagetaxi0001 # only letters and numbers allowed in name
export storageContainer=storagecontainer-taxi
export dataFactory=adf-taxi-trip-0023

# Create a resource group.
az group create --name $resourceGroup --location $location

# Create a storage account.
az storage account create --resource-group $resourceGroup \
    --name $storageAccount --location $location

# Create a container in the storage account.
az storage container create --resource-group $resourceGroup --name $storageContainer \
    --account-name $storageAccount --auth-mode key
	
# Create a data factory.
az datafactory create --resource-group $resourceGroup \
    --factory-name $dataFactory

az datafactory show --resource-group $resourceGroup \
    --factory-name $dataFactory