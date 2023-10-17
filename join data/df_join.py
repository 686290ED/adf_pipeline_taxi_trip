# sudo apt-get install pip
# pip install azure-identity
# pip install azure-mgmt-storage
# pip install azure-mgmt-resource
# pip install azure-storage-blob
# curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
# az login
import pandas as pd
from azure.identity import AzureCliCredential
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.storage.blob import ContainerClient
from io import BytesIO
from azure.cli.core import get_default_cli

az_cli = get_default_cli()
spID = "<sp_id>"
tenantID = "<tenant_id>"
az_cli.invoke(['login', '--service-principal', '--username', spID, '--tenant', tenantID,
               '--password', 'tmpub1yw9k2.pem'])

# Acquire a credential object using CLI-based authentication.
credential = AzureCliCredential()

# Retrieve subscription ID from environment variable.
subscription_id = '<subscription_id>'

# Obtain the management object for resources.
resource_client = ResourceManagementClient(credential, subscription_id)
storage_client = StorageManagementClient(credential, subscription_id)
storage_account_name = 'storagetaxi0001'

# Constants we need in multiple places: the resource group name and the region
# in which we provision resources. You can change these values however you want.
rg_name = 'rg_adf'
location = "westus2"
container_name = "storagecontainer-taxi"

# Step 3: Retrieve the account's primary access key and generate a connection string.
# keys = storage_client.storage_accounts.list_keys(rg_name, storage_account_name)
# print(f"Primary key for storage account: {keys.keys[0].value}")
# conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;" \
#               f"AccountName={storage_account_name};AccountKey={keys.keys[0].value}"

az_cli.invoke(['storage', 'account', 'show-connection-string', '--resource-group', rg_name,
    '--name', storage_account_name, '--key', 'key1', '--output', 'tsv'])
conn_string = az_cli.result.result['connectionString']

print(f"Connection string: {conn_string}")
# Establish connection with the blob storage account
types = ['trip', 'coord_station', 'weather']
blob_name = {'trip': 'trip_weather_original/taxi_trip_2022-08-01_2023-09-30.txt',
             'coord_station': 'trip_weather_processed/chicago_nearest_station/',
             'weather': ['trip_weather_original/weather_2022_2022-08-01_2023-09-30.txt',
                         'trip_weather_original/weather_2023_2022-08-01_2023-09-30.txt']}

container_client = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container_name)
# [c.name for c in container_client.list_blobs() if c.contains(blob_name[type])]
dfs = dict()
type = 'coord_station'
blob_loc = [c.name for c in container_client.list_blobs() if blob_name[type] in c.name and c.name.endswith('csv')][0]
blob_content = container_client.download_blob(blob=blob_loc).readall()
dfs[type] = pd.read_csv(BytesIO(blob_content))

type = 'weather'
df_list = []
for file in blob_name[type]:
    blob_content = container_client.download_blob(blob=file).readall()
    df_list.append(pd.read_csv(BytesIO(blob_content)))
dfs[type] = pd.concat(df_list)

type = 'trip'
blob_content = container_client.download_blob(blob=blob_name[type]).readall()
f_out = 'taxi_trip_year.txt'
with open(f_out, 'wb') as f:
    f.write(BytesIO(blob_content).getbuffer())
del blob_content
dfs[type] = pd.read_csv(f_out)

dfs['weather'] = dfs['weather'].loc[dfs['weather']['element'] == 'PRCP']
dfs['trip']['date'] = dfs['trip']['trip_start_timestamp'].str[0:10]
dfs['trip']['time'] = (dfs['trip']['trip_start_timestamp'].str[11:13] +
                       dfs['trip']['trip_start_timestamp'].str[14:16]).astype(int)
dfs['weather']['time'] = dfs['weather']['time'].fillna(1200).astype(int)
df_trip_weather = pd.merge_asof((dfs['trip'].loc[:, ['unique_key', 'date', 'time', 'pickup_location']].merge(\
    dfs['coord_station'].loc[:, ['trip_location', 'name', 'id']],
    how='left', left_on='pickup_location', right_on='trip_location')).sort_values('time'),
    dfs['weather'].sort_values('time'), by=['id', 'date'], on='time', direction='backward')

df_trip_add_weather = dfs['trip'].merge(df_trip_weather.drop(columns='pickup_location'), on='unique_key')

f_out = 'taxi_trip_weather_combined.txt'
df_trip_add_weather.to_csv(f_out, index=False)
del df_trip_add_weather
with open(f_out, "rb") as data:
    container_client.upload_blob(
        'trip_weather_processed/trip_weather_combined/trip_weather_combined_history.csv', data, overwrite=True)