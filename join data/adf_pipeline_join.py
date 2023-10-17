from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *


def create_join_activity():
    ls_batch = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_batch)
    ls_blob = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_blob)
    act_name = 'join_trip_with_weather'
    custom_activity = CustomActivity (name=act_name, linked_service_name=ls_batch,
                                      command='python3 df_join.py',
                                      resource_linked_service=ls_blob,
                                      folder_path="storagecontainer-taxi/input"
                                      )
    return custom_activity


def main():
    # Azure subscription ID
    subscription_id = '<subscription_id>'

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = 'rg_adf'

    # The data factory name. It must be globally unique.
    df_name = 'adf-taxi-trip-0023'

    location = 'westus2'
    ls_name_batch = 'lsbatch'
    ls_name_blob = 'lsblob'
    # Specify your Active Directory client ID, client secret, and tenant ID
    # https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app
    credentials = ClientSecretCredential(client_id='<client_id>',
                                         client_secret='<client_secret>',
                                         tenant_id='<tenant_id>')
    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    p_name = 'pl_join_trip_with_weather'
    p_obj = PipelineResource(activities=[create_join_activity()])
    adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)

    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name)

main()

