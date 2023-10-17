from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta


def create_split_activity(type):
    ds_name_source = 'ds_blob_combined_' + type
    ds_name_sink = 'ds_blob_splitted_' + type
    container = 'storagecontainer-taxi'
    ds_ls_blob = LinkedServiceReference(type="LinkedServiceReference", reference_name=ls_name_blob)
    folder_path_sink = 'trip_weather_splitted/' + type + '/'
    # specify format to disable binary copy
    location = AzureBlobStorageLocation(
        folder_path=folder_path_sink, container=container)
    ds_sink_prop = DatasetResource(properties=DelimitedTextDataset(
        linked_service_name=ds_ls_blob, location=location, column_delimiter=",",
        escape_char="\\", first_row_as_header=True))
    ds_sink = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_name_sink, ds_sink_prop)
    dsin_ref = DatasetReference(type="DatasetReference", reference_name=ds_name_source)
    dsOut_ref = DatasetReference(type="DatasetReference", reference_name=ds_name_sink)
    gen_month_column = Transformation(name='deriveymcolumn')
    dt_col = {'taxi': 'trip_start_timestamp', 'weather': 'date'}
    columns = {'taxi': [
                "          unique_key as string,",
                "          taxi_id as string,",
                "          trip_start_timestamp as string,",
                "          trip_end_timestamp as string,",
                "          trip_seconds as string,",
                "          trip_miles as string,",
                "          pickup_census_tract as string,",
                "          dropoff_census_tract as string,",
                "          pickup_community_area as string,",
                "          dropoff_community_area as string,",
                "          fare as string,",
                "          tips as string,",
                "          tolls as string,",
                "          extras as string,",
                "          trip_total as string,",
                "          payment_type as string,",
                "          company as string,",
                "          pickup_latitude as string,",
                "          pickup_longitude as string,",
                "          pickup_location as string,",
                "          dropoff_latitude as string,",
                "          dropoff_longitude as string,",
                "          dropoff_location as string,",
                "          yr as string"],
              'weather': ["          id as string,",
                          "          date as string,"
                          "          element as string,",
                          "          value as string,",
                          "          time as string"]}
    script_lines = [
                "parameters{",
                "     filename as string (\"taxi_trip_@{pipeline().parameters.start_ym}_@{pipeline().parameters.end_ym}.txt\")",
                "}",
                "source(output("] + columns[type] + \
                ["     ),",
                "     allowSchemaDrift: true,",
                "     validateSchema: false,",
                "     ignoreNoFilesFound: false) ~> combined" + type,
                "combined" + type + " derive(ym = substring(toString(" + dt_col[type] + "), 1, 7)) ~> deriveymcolumn",
                "deriveymcolumn sink(allowSchemaDrift: true,",
                "     validateSchema: false,",
                "     truncate: true,",
                "     skipDuplicateMapInputs: false,",
                "     skipDuplicateMapOutputs: false,",
                "     partitionBy('key',",
                "          0,",
                "          ym",
                "     )) ~> splitted" + type
            ]
    data_flow_resource = DataFlowResource(
        properties=MappingDataFlow(
            description="split data by month",
            sources=[DataFlowSource(name='combined'+type, dataset=dsin_ref)],
            sinks=[DataFlowSink(name='splitted'+type, dataset=dsOut_ref)],
            transformations=[gen_month_column],
            script_lines=script_lines
        )
    )
    dataflow_name = 'split_monthly_' + type
    adf_client.data_flows.create_or_update(
        rg_name, df_name, dataflow_name, data_flow_resource)
    dataflow_activity = ExecuteDataFlowActivity(
        name='activity_split_data_monthly_' + type, data_flow=DataFlowReference(
            type="DataFlowReference", reference_name=dataflow_name),
        depends_on=[]
    )
    return dataflow_activity


def main():
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

    types = ['taxi', 'weather']
    params_for_pipeline = {
        'start_ym': {'type': 'String'},
        'start_year': {'type': 'String'},
        'end_ym': {'type': 'String'}
    }
    pls = dict()
    for type in types:
        p_name = 'pl_split_' + type
        p_obj = PipelineResource(
            activities=[create_split_activity(type)], parameters=params_for_pipeline)
        pl = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)

    end_dt = (datetime.today().replace(day=1) - timedelta(days=1))
    # 13 months
    start_dt = (end_dt - timedelta(days=397)).replace(day=1)
    start_year = start_dt.year
    end_year = end_dt.year
    start_dt = start_dt.strftime('%Y-%m-%d')
    end_dt = end_dt.strftime('%Y-%m-%d')

    for type in types:
        p_name = 'pl_split_' + type
        start_year_type = start_year
        while True:
            print(start_year_type)
            run_response = adf_client.pipelines.create_run(
                rg_name, df_name, p_name,
                parameters={'start_ym': start_dt, 'start_year': start_year_type, 'end_ym': end_dt})
            if start_year_type == end_year or type == 'taxi':
                break
            start_year_type += 1

main()