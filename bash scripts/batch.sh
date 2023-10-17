export resourceGroup=rg_adf
export location=westus2
export storageAccount=storagetaxi0001 # only letters and numbers allowed in name
export storageContainer=storagecontainer-taxi
export dataFactory=adf-taxi-trip-0023
# blob key
export blobKeyVault=kvblob0023
export batchAccount=batch0023

# Create a batch account 
export batchAccountId=$(az batch account create --location $location \
                                                --name $batchAccount \
                                                --resource-group $resourceGroup \
												--query id \
												--output tsv)

# batch vm start job
# reference https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-service-principal
# reference https://learn.microsoft.com/en-us/cli/azure/azure-cli-sp-tutorial-3
# reference https://stackoverflow.com/questions/62983437/authenticating-azure-cli-with-python-sdk
# reference https://stackoverflow.com/questions/73830524/attributeerror-module-lib-has-no-attribute-x509-v-flag-cb-issuer-check
# curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash    to install az cli
# pip install pyopenssl --upgrade
# /bin/bash -c "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3 get-pip.py && python3 -m pip install pyopenssl --upgrade && python3 -m pip install azure-cli pandas azure-identity azure-mgmt-storage azure-mgmt-resource azure-storage-blob"


# login to az account using program
# https://learn.microsoft.com/en-us/cli/azure/azure-cli-sp-tutorial-3#work-with-azure-key-vault
# Create a service principal storing the certificate in Azure Key Vault
export servicePrincipalName=sp_adf_key
export certificateName=certificateadfkey
# blob key
export spKeyVault=kvsp0023
export dataFactory=adf-taxi-trip-0023
export resourceGroup=rg_adf
export location=westus2
export resourceGroup=rg_adf

####################################################################################################
# export scope=$(az datafactory show --name $dataFactory --resource-group $resourceGroup --query id --output tsv)
## Create a key vault
# az keyvault create --name $spKeyVault --resource-group $resourceGroup --location $location
# az ad sp create-for-rbac --name $servicePrincipalName \
                         # --role "Data Factory Contributor" \
                         # --scopes $scope \
                         # --create-cert \
                         # --cert $certificateName \
                         # --keyvault $spKeyVault
####################################################################################################
		
az ad sp create-for-rbac --name $servicePrincipalName \
                         --role "Data Factory Contributor" \
                         --scopes $scope \
                         --create-cert
export spID=$(az ad sp list --display-name sp_adf_key --query [0].appId --output tsv)	
export rg_scope=$(az group show --name $resourceGroup --query id --output tsv)
az role assignment create --assignee $spID \
                          --role Contributor \
						  --scope $rg_scope
					 	
export spID="<service_printcipal_id>"		
export tenantID="<tenant_id>"
# Sign in with a service principal using a certificate	
# az login --service-principal \
         # --username $spID \
         # --tenant $tenantID \
         # --password /home/azureuser/tmpub1yw9k2.pem


