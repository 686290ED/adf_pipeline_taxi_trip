
"""
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
"""
# %pip install geopy

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from geopy.distance import geodesic
from pyspark.sql.types import FloatType
from pyspark.sql import Window

container_name = 'storagecontainer-taxi'
storage_account_name = 'storagetaxi0001'
scope = "key-vault-secret"
blob_key = "storageKey"

spark = SparkSession.builder.getOrCreate()
spark.conf.set(
    "fs.azure.account.key.storagetaxi0001.dfs.core.windows.net",
    dbutils.secrets.get(scope=scope, key=blob_key))

# df = spark.read.csv("abfss://storagecontainer-taxi@storagetaxi0001.dfs.core.windows.net/trip_weather_original/weather_station_list.txt").collect()

# dbutils.fs.mount(
# source = "abfss://" + container_name + "@" + sotrage_account_name + ".dfs.core.windows.net",
# mount_point="/mnt/taxi_weather")
# extra_configs={"fs.azure.account.key.storagecontainer-taxi.dfs.core.windows.net":
#                                    dbutils.secrets.get(scope=scope, key=blob_key)})


path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
end_dt = (datetime.today().replace(day=1) - timedelta(days=1))
# 13 months
start_dt = (end_dt - timedelta(days=397)).replace(day=1)
start_year = start_dt.year
end_year = end_dt.year
start_dt = start_dt.strftime('%Y-%m-%d')
end_dt = end_dt.strftime('%Y-%m-%d')
df_taxi = spark.read.option("header", True).csv(path + f"trip_weather_original/taxi_trip_{start_dt}_{end_dt}.txt")
df_weather_station = spark.read.option("header", True).csv(path + f"trip_weather_original/weather_station_list.txt")
# "abfss://storagecontainer-taxi@storagetaxi0001.dfs.core.windows.net/trip_weather_original/weather_station_list.txt"


df_taxi_coord_pickup = df_taxi.select(col('pickup_location').alias('location'),
                                      col('pickup_latitude').alias('latitude'),
                                      col('pickup_longitude').alias('longitude'))
df_taxi_coord_pickup = df_taxi_coord_pickup.filter(df_taxi_coord_pickup.location.isNotNull())
df_taxi_coord_dropoff = df_taxi.select(col('dropoff_location').alias('location'),
                                       col('dropoff_latitude').alias('latitude'),
                                       col('dropoff_longitude').alias('longitude'))
df_taxi_coord_dropoff = df_taxi_coord_dropoff.filter(df_taxi_coord_dropoff.location.isNotNull())
df_taxi_coord = df_taxi_coord_pickup.union(df_taxi_coord_dropoff).dropDuplicates()

df_taxi_coord_station = df_taxi_coord.select(col('location').alias('trip_location'),
                                             col('latitude').cast('float').alias('trip_latitude'),
                                             col('longitude').cast('float').alias('trip_longitude'))\
                        .crossJoin(df_weather_station.select(col('name'), col('id'), col('state'),
                                                             col('latitude').cast('float').alias('station_latitude'),
                                                             col('longitude').cast('float').alias('station_longitude')))
df_taxi_coord_station_filtered = df_taxi_coord_station.where(
    df_taxi_coord_station.station_latitude.between(expr('trip_latitude - 0.7'), expr('trip_latitude + 0.7')) &
    df_taxi_coord_station.station_longitude.between(expr('trip_longitude - 0.7'), expr('trip_longitude + 0.7')))


@udf(returnType=FloatType())
def geodesic_udf(a, b):
    return geodesic(a, b).m


df_taxi_coord_station_filtered = df_taxi_coord_station_filtered.withColumn(
    'distance', geodesic_udf(array("trip_latitude", "trip_longitude"), array("station_latitude", "station_longitude")))

w = Window.partitionBy("trip_location").orderBy(col("distance"))

df_nearest_station = df_taxi_coord_station_filtered.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1).drop("rn")

df_nearest_station.write.csv(path + f"trip_weather_processed/chicago_nearest_station", header=True, mode='overwrite')
