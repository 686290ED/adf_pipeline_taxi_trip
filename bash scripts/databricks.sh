export resourceGroup=rg_adf
export location=westus2
export storageAccount=storagetaxi0001 # only letters and numbers allowed in name
export storageContainer=storagecontainer-taxi
export dataFactory=adf-taxi-trip-0023
# linked service
export linkedServiceBigQuery=lsbigquery
export linkedServiceBlob=lsblob
# blob key
export blobKeyVault=kvblob0023

# Create a databricks workspace
az databricks workspace create --resource-group $resourceGroup --name db_adf --location $location --sku standard

# Create a linked service for databricks from UI

### Get access key to the blob for databricks access	
# get scope of the blob and grant the current account access to the blob
export blobId=$(az storage account show --name $storageAccount --query id --output tsv)
export userObjectId=$(az ad signed-in-user show --query id -o tsv)
az role assignment create --assignee-object-id $userObjectId \
                          --assignee-principal-type User \
						  --role "Storage Blob Data Contributor" \
						  --scope $blobId
						  
# Get access key to the blob
az storage blob list --account-name $storageAccount --container-name $storageContainer --output table --auth-mode login
export blobKey=$(az storage account keys list -g $resourceGroup -n $storageAccount --query "[?keyName == 'key1'].value" --output tsv)

# Create a key vault with the blob key1
az keyvault create --name $blobKeyVault --resource-group $resourceGroup --location $location
az keyvault secret set --vault-name $blobKeyVault --name storageKey --value $blobKey
az keyvault show --name $blobKeyVault --query id --output tsv
