from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

container_name = 'storagecontainer-taxi'
storage_account_name = 'storagetaxi0001'
scope = "key-vault-secret"
blob_key = "storageKey"

spark = SparkSession.builder.getOrCreate()
spark.conf.set(
    "fs.azure.account.key.storagetaxi0001.dfs.core.windows.net",
    dbutils.secrets.get(scope=scope, key=blob_key))

df_combined = spark.read.option("header", True).csv("abfss://storagecontainer-taxi@storagetaxi0001.dfs.core.windows.net/trip_weather_processed/trip_weather_combined/trip_weather_combined_history.csv")

df_combined = df_combined.drop('pickup_location_y').withColumnRenamed('pickup_location_x', 'pickup_location')

# most common routes
df_combined.filter((col("pickup_location").isNotNull()) & (col("dropoff_location").isNotNull())) \
    .groupby('pickup_location', 'dropoff_location')\
    .agg(count('trip_total').alias('cnt'), avg('trip_total').alias('avg_trip_total'))\
    .sort(desc('cnt')).limit(10).take(10)

# extreme rate
df_combined.withColumn('second_rate', col('trip_total') / col('trip_seconds'))\
    .approxQuantile('second_rate', [0.25, 0.5, 0.75, 0.9, 0.99], 0.02)

# top income taxi
taxi_top_income = df_combined.groupby('taxi_id').agg(sum('trip_total').alias('total_income'))\
    .sort(desc('total_income')).limit(100).select('taxi_id', 'total_income')
taxi_top_income.rdd.flatMap(lambda x: x).collect()

# top income taxis and their income distribution
df_taxi_top_income = df_combined.filter(col('taxi_id').isin(taxi_top_income['taxi_id']))
df_taxi_percentile = df_taxi_top_income.select('taxi_id', 'trip_total')\
    .groupby('taxi_id').agg(expr('int(percentile(trip_total, 0.25))').alias('percentile_25'),
                            expr('int(percentile(trip_total, 0.5))').alias('percentile_50'),
                            expr('int(percentile(trip_total, 0.75))').alias('percentile75'))
df_taxi_percentile.take(10)

# daily taxi income distribution
df_taxi_daily_percentile = df_combined.withColumn('trip_total_f', df_combined.trip_total.cast("float"))\
    .withColumn('date', substring(col('trip_start_timestamp'), 1, 10))\
    .groupby('taxi_id', 'date')\
    .agg(sum('trip_total_f').alias('trip_total_daily'))\
    .groupby('taxi_id').agg(expr('int(percentile(trip_total_daily, 0.25))').alias('percentile_25'),
                            expr('int(percentile(trip_total_daily, 0.5))').alias('percentile_50'),
                            expr('int(percentile(trip_total_daily, 0.75))').alias('percentile75'))

df_taxi_daily_percentile.take(10)





















