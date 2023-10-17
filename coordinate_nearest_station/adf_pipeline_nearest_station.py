from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *


def create_notebook_activity():
    ls_databricks = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_databricks)
    act_name = 'notebook_get_nearest_station'
    notebook_activity = DatabricksNotebookActivity(name=act_name, linked_service_name=ls_databricks,
                                               notebook_path="/Users/folder/get_nearest_station",
                                               )
    return notebook_activity


def main():
    # Azure subscription ID
    subscription_id = '<subscription_id>'

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = 'rg_adf'

    # The data factory name. It must be globally unique.
    df_name = 'adf-taxi-trip-0023'

    location = 'westus2'

    # Specify your Active Directory client ID, client secret, and tenant ID
    # https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app
    credentials = ClientSecretCredential(client_id='<client_id>',
                                         client_secret='<client_secret>',
                                         tenant_id='<tenant_id>')
    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    ls_name_databricks = 'lsdatabricks'

    p_name = 'pl_nearest_station'
    p_obj = PipelineResource(activities=[create_notebook_activity()])
    adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)

    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name)

main()
