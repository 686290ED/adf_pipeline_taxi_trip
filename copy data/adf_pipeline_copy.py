# https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#register-an-application-with-azure-ad-and-create-a-service-principal

from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta


def get_copy_query(type):
    if type == 'taxi':
        query = 'SELECT * except (second_rate), extract(year from trip_start_timestamp) as yr ' \
                'FROM (SELECT *, trip_total / trip_seconds as second_rate' \
                ' FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` ' \
                'WHERE trip_start_timestamp >= "@{pipeline().parameters.start_ym}" ' \
                'AND trip_start_timestamp <= "@{pipeline().parameters.end_ym}" ' \
                'AND trip_seconds is not null and trip_seconds > 60) rate ' \
                'WHERE second_rate <= 1'
    elif type == 'weather':
        query = 'SELECT id, date, element, value, time ' \
                'FROM `bigquery-public-data.ghcn_d.ghcnd_@{pipeline().parameters.start_year}` ' \
                'WHERE date >= "@{pipeline().parameters.start_ym}" AND date <= "@{pipeline().parameters.end_ym}" '\
                'AND id in '\
                '(SELECT id FROM `bigquery-public-data.ghcn_d.ghcnd_stations` ' \
                'WHERE latitude BETWEEN 41.651921576 - 0.05 AND 42.021223593 + 0.05 ' \
                'AND longitude BETWEEN -87.913624596 - 0.05 AND -87.530712484 + 0.05) '
    elif type == 'weather_station':
        query = 'SELECT id, latitude, longitude, state, name FROM `bigquery-public-data.ghcn_d.ghcnd_stations` ' \
                'WHERE latitude BETWEEN 41.651921576 - 0.05 AND 42.021223593 + 0.05 ' \
                'AND longitude BETWEEN -87.913624596 - 0.05 AND -87.530712484 + 0.05'
    return query


def create_copy_activity(type):
    ds_name_bigquery = 'ds_bigquery_' + type
    ds_ls_bigquery = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_bigquery)
    ds_source_prop = DatasetResource(properties=GoogleBigQueryObjectDataset(linked_service_name=ds_ls_bigquery))
    ds_source = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name_bigquery, ds_source_prop)
    ds_name_blob = 'ds_blob_combined_' + type
    ds_ls_blob = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_blob)
    container = 'storagecontainer-taxi'
    blob_path = 'trip_weather_original'
    location = AzureBlobStorageLocation(
        folder_path=blob_path, file_name="@dataset().filename", container=container)
    # specify format to disable binary copy
    parameter = {'filename': ParameterSpecification(type='string', default_value=filenames[type])}
    ds_sink_prop = DatasetResource(properties=DelimitedTextDataset(
        linked_service_name=ds_ls_blob, location=location, column_delimiter=",", escape_char="\\",
        parameters=parameter, first_row_as_header=True))
    ds_sink = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name_blob, ds_sink_prop)
    # Create a copy activity
    act_name = 'copy_bigquery_to_blob_' + type
    blob_source = GoogleBigQuerySource(query=get_copy_query(type))
    blob_sink = DelimitedTextSink()
    dsin_ref = DatasetReference(type="DatasetReference", reference_name=ds_name_bigquery)
    dsOut_ref = DatasetReference(type="DatasetReference", reference_name=ds_name_blob)
    copy_activity = CopyActivity(name=act_name, source=blob_source, sink=blob_sink,
                                 description="Copy " + type + " data", inputs=[dsin_ref], outputs=[dsOut_ref])
    return copy_activity


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

    ls_name_bigquery = 'lsbigquery'
    ls_name_blob = 'lsblob'

    filenames = {'taxi': 'taxi_trip_@{pipeline().parameters.start_ym}_@{pipeline().parameters.end_ym}.txt',
                 'weather': 'weather_@{pipeline().parameters.start_year}_@{pipeline().parameters.start_ym}_'
                            '@{pipeline().parameters.end_ym}.txt',
                 'weather_station': 'weather_station_list.txt'}

    # Create BigQuery datasets and Azure blob datasets.
    types = ['taxi', 'weather', 'weather_station']
    params_for_pipeline = {
        'start_ym': {'type': 'String'},
        'start_year': {'type': 'String'},
        'end_ym': {'type': 'String'}
    }
    pls = {}
    for type in types:
        p_name = 'pl_copy_' + type
        p_obj = PipelineResource(
            activities=[create_copy_activity(type)], parameters=params_for_pipeline)
        pls[type] = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)

    end_dt = (datetime.today().replace(day=1) - timedelta(days=1))
    # 13 months
    start_dt = (end_dt - timedelta(days=397)).replace(day=1)
    start_year = start_dt.year
    end_year = end_dt.year
    start_dt = start_dt.strftime('%Y-%m-%d')
    end_dt = end_dt.strftime('%Y-%m-%d')

    for type in types:
        p_name = 'pl_copy_' + type
        start_year_type = start_year
        while True:
            print(start_year_type)
            run_response = adf_client.pipelines.create_run(
                rg_name, df_name, p_name,
                parameters={'start_ym': start_dt, 'start_year': start_year_type, 'end_ym': end_dt})
            if start_year_type == end_year or type in ['taxi', 'weather_station']:
                break
            start_year_type += 1


# Start the main method
main()
