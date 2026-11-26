# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a0e81625-9b2f-4084-b2a7-c27c95a5d6d9",
# META       "default_lakehouse_name": "Project_Lakehouse",
# META       "default_lakehouse_workspace_id": "742d1f01-1b60-4625-91e5-0f5292a76adc",
# META       "known_lakehouses": [
# META         {
# META           "id": "a0e81625-9b2f-4084-b2a7-c27c95a5d6d9"
# META         },
# META         {
# META           "id": "fcce5f35-a431-4dad-8439-99dadb00053b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, IntegerType, DateType


def readTableToDataFrame(table_name):
    df = spark.read.synapsesql(table_name)
    return df

def replaceSubstringInColumnNames(df, sub_to_replace, replacement_sub):
    names = df.schema.names
    for name in names:
        df = df.withColumnRenamed(name, name.replace(sub_to_replace, replacement_sub))
    return df

def replaceDotsInColumnNames(df):
    return replaceSubstringInColumnNames(df, '.', '_')

def dropColumnsByNameSubstrings(df, columns: list):
    names = df.schema.names
    for column in columns:
        for name in names:
            if name.count(column) != 0:
                df = df.drop(name)
    return df

def roundValuesOfColumnsTo(df, columns, rounding):
    for column in columns:
        df = df.withColumn(column, sf.round(df[column], rounding))
    return df


def writeTableOut(source, dest):
    source.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(dest)

def convertTimestampColumnIntoDateDimension(table, column_name: str):
    date_dim = table.select(sf.to_date(column_name), sf.year(column_name), sf.quarter(column_name), sf.dayofyear(column_name), sf.month(column_name), sf.dayofmonth(column_name), sf.dayofweek(column_name), sf.weekofyear(column_name))
    date_dim = replaceSubstringInColumnNames(date_dim, '(' + column_name + ')', '')
    date_dim = date_dim.withColumnRenamed('to_date', 'date')
    date_dim = replaceSubstringInColumnNames(date_dim, 'of', '_of_')
    date_dim = date_dim.dropDuplicates()
    return date_dim

def converAndJoinTimestampColumnToDateDimension(table, column_name:str, date_dim = None):
    schema = StructType([
        StructField('date', DateType(), False),
        StructField('year', IntegerType(), False),
        StructField('quarter', IntegerType(), False),
        StructField('day_of_year', IntegerType(), False),
        StructField('month', IntegerType(), False),
        StructField('day_of_month', IntegerType(), False),
        StructField('day_of_week', IntegerType(), False),
        StructField('week_of_year', IntegerType(), False)
    ])
    if date_dim == None:
        date_dim = spark.createDataFrame([], StructType(schema))
    date_dim = date_dim.join(convertTimestampColumnIntoDateDimension(table, column_name),['date', 'year', 'quarter', 'day_of_year', 'month', 'day_of_month', 'day_of_week', 'week_of_year'], how = 'right')
    date_dim = date_dim.dropDuplicates()

    return date_dim



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = readTableToDataFrame("Project_Lakehouse.OpenAQ_AddedCoordinates.OpenAQ_AddedCoordinates_Combined")
df = dropColumnsByNameSubstrings(df, ['meta', 'flag', 'utc', 'coverage','summary' , 'displayName','results_coordinates', 'results_period_label', 'results_coverage_expectedInterval', 'results_coverage_expectedCount', 'results_period_interval', 'results_coverage_observedCount'])
df = replaceSubstringInColumnNames(df, 'results_', '')
df = replaceSubstringInColumnNames(df, 'F', '_f')
df = replaceSubstringInColumnNames(df, 'T', '_t')


from pyspark.sql import functions as sf

split_col = sf.split(df['coordinates'], ' ')
df = df.withColumn('coordinates_latitude', split_col.getItem(0).cast("double"))
df = df.withColumn('coordinates_longitude', split_col.getItem(1).cast("double"))
df = roundValuesOfColumnsTo(df, ['coordinates_latitude', 'coordinates_longitude'], 12)

df = df.drop('coordinates')

df = df.join(roundValuesOfColumnsTo(readTableToDataFrame('Project_Lakehouse.dbo.locationsTable'), ['coordinates_latitude', 'coordinates_longitude'], 12), on=['coordinates_latitude', 'coordinates_longitude'],how = 'full')
#df = df.join(spark.read.synapsesql('Project_Lakehouse.dbo.sensor_locations_table'), on=['coordinates_latitude', 'coordinates_longitude'],how = 'full')
df = df.join(roundValuesOfColumnsTo(readTableToDataFrame('Project_Lakehouse.dbo.broader_sensors_location_table'), ['coordinates_latitude', 'coordinates_longitude'], 12), on=['coordinates_longitude', 'coordinates_latitude'],how = 'full')

df = replaceSubstringInColumnNames(df, 'LocationID', 'sensor_location_zone_id')
df = df.withColumnRenamed('zone', 'sensor_location_zone_name')
df = df.fillna({'sensor_location_zone_name': '265', 'sensor_location_zone_id': '265'})

df = df.dropna(subset= ['parameter_id', 'value', 'period_datetime_to_local'])
df = df.dropDuplicates()

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parameter_df = df.select('parameter_id', 'parameter_name', 'parameter_units').dropDuplicates()
display(parameter_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensor_df = df.select('sensor_id', 'sensor_zone_id', 'sensor_location_zone_id', 'coordinates_longitude', 'coordinates_latitude').dropDuplicates().orderBy('sensor_id')
sensor_df = sensor_df.withColumnRenamed('sensor_location_zone_id', 'taxi_zone_id')
sensor_df = sensor_df.withColumn('taxi_zone_id', sensor_df['taxi_zone_id'].cast('int'))

display(sensor_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measurements_df = df.select('value', 'parameter_id', 'period_datetime_from_local', 'period_datetime_to_local', 'sensor_id').withColumn('measurement_id', sf.monotonically_increasing_id())
measurements_df = replaceSubstringInColumnNames(measurements_df, 'period_', '')
measurements_df = replaceSubstringInColumnNames(measurements_df, '_local', '')
measurements_df = measurements_df.withColumn('datetime_from', sf.to_timestamp(measurements_df['datetime_from']))
measurements_df = measurements_df.withColumn('datetime_to', sf.to_timestamp(measurements_df['datetime_to']))

display(measurements_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_df = converAndJoinTimestampColumnToDateDimension(measurements_df, 'datetime_from')
date_df = converAndJoinTimestampColumnToDateDimension(measurements_df, 'datetime_to', date_df)
display(date_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measurements_df = measurements_df.withColumn('date', sf.to_date(measurements_df['datetime_from']))
measurements_df = measurements_df.withColumn('hour_from', sf.hour(measurements_df['datetime_from']))
measurements_df = measurements_df.withColumn('hour_to', sf.hour(measurements_df['datetime_to']))
measurements_df = measurements_df.drop('datetime_from')
measurements_df = measurements_df.drop('datetime_to')

display(measurements_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi = readTableToDataFrame('Project_Lakehouse.dbo.TaxiLocations')
taxi_to_sensors = readTableToDataFrame('Project_Lakehouse.dbo.taxi_to_sensors_locations_table')
taxi = taxi.join(taxi_to_sensors, ['LocationID', 'zone'], 'full')
taxi = taxi.withColumnRenamed('LocationID', 'taxi_zone_id')
taxi = taxi.fillna(0, subset='sensor_zone_id')
taxi = taxi.withColumn('taxi_zone_id', taxi['taxi_zone_id'].cast('int'))

display(taxi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensor_zones_df = sensor_df.select('sensor_zone_id').groupBy('sensor_zone_id').count()
sensor_zones_df = sensor_zones_df.withColumnRenamed('count', 'number_of_sensors').orderBy('sensor_zone_id')
display(sensor_zones_df)
sensor_df = sensor_df.drop('taxi_zone_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green = readTableToDataFrame('Project_Lakehouse.dbo.Taxi_Raw_green')
green = replaceSubstringInColumnNames(green, 'lpep_', '')
yellow = readTableToDataFrame('Project_Lakehouse.dbo.Taxi_Raw_yellow')
yellow = replaceSubstringInColumnNames(yellow, 'tpep_', '')

green_and_yellow = green.join(yellow ,list(set(green.schema.names)&set(yellow.schema.names)) ,'full').drop_duplicates()

display(green_and_yellow.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhv = readTableToDataFrame('Project_Lakehouse.dbo.Taxi_Raw_fhv')
fhv = fhv.withColumnsRenamed({'DOlocationID': 'DOLocationID', 'PULocationID': 'PULocationID', 'dropOff_datetime': 'dropoff_datetime'})
fhvhv = readTableToDataFrame('Project_Lakehouse.dbo.Taxi_Raw_fhvhv')
fhv_and_fhvhv = fhv.join(fhvhv ,list(set(fhv.schema.names)&set(fhvhv.schema.names)) ,'full').drop_duplicates()

display(fhv_and_fhvhv.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhv_and_fhvhv = fhv_and_fhvhv.withColumnsRenamed({'tips': 'tip_amount', 'tolls': 'tolls_amount', 'trip_miles': 'trip_distance', 'base_passenger_fare': 'fare_amount'})
fhv_and_fhvhv = fhv_and_fhvhv.withColumn('total_amount', fhv_and_fhvhv['fare_amount'] + fhv_and_fhvhv['tolls_amount']+ fhv_and_fhvhv['tip_amount'] + fhv_and_fhvhv['airport_fee'] + fhv_and_fhvhv['congestion_surcharge']+ fhv_and_fhvhv['bcf']+ fhv_and_fhvhv['sales_tax'])

all_taxi_df = green_and_yellow.join(fhv_and_fhvhv ,list(set(fhv_and_fhvhv.schema.names)&set(green_and_yellow.schema.names)) ,'full').drop_duplicates()

all_taxi_df = dropColumnsByNameSubstrings(all_taxi_df, ['trip_time', 'on_scene_datetime','request_datetime','license', 'Flag', 'flag', 'base', 'type', 'count'])
all_taxi_df = all_taxi_df.drop('VendorID', 'RatecodeID')
all_taxi_df = replaceSubstringInColumnNames(all_taxi_df, 'DOL', 'dropoff_l')
all_taxi_df = replaceSubstringInColumnNames(all_taxi_df, 'PUL', 'pickup_l')
all_taxi_df = replaceSubstringInColumnNames(all_taxi_df, 'ID', '_id')

date_df = converAndJoinTimestampColumnToDateDimension(all_taxi_df, 'pickup_datetime', date_df)
date_df = converAndJoinTimestampColumnToDateDimension(all_taxi_df, 'dropoff_datetime', date_df)

all_taxi_df = all_taxi_df.withColumn('date_from', sf.to_date(all_taxi_df['pickup_datetime']))
all_taxi_df = all_taxi_df.withColumn('hour_from', sf.hour(all_taxi_df['pickup_datetime']))
all_taxi_df = all_taxi_df.withColumn('date_to', sf.to_date(all_taxi_df['dropoff_datetime']))
all_taxi_df = all_taxi_df.withColumn('hour_to', sf.hour(all_taxi_df['dropoff_datetime']))
all_taxi_df = all_taxi_df.withColumn('trip_id', sf.monotonically_increasing_id())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx = readTableToDataFrame('Project_Lakehouse.dbo.EUR_USD')
fx = fx.withColumn('TIME_PERIOD', sf.to_date(fx['TIME_PERIOD']))
fx = fx.select(sf.expr('*')).where(sf.year(fx['TIME_PERIOD']) >= 2015)
fx = fx.dropna().dropDuplicates()
fx = fx.withColumn('OBS_VALUE', fx['OBS_VALUE'].cast('float'))

display(fx)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp = readTableToDataFrame('Project_Lakehouse.dbo.NYC_GDP')
gdp = gdp.select(sf.expr('*')).where(sf.year(gdp['date']) >= 2015)

display(gdp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measurements_df.schema['measurement_id'].nullable = False
sensor_df.schema['sensor_id'].nullable = False
parameter_df.schema['parameter_id'].nullable = False
taxi.schema['taxi_zone_id'].nullable = False
date_df.schema['date'].nullable = False
all_taxi_df.schema['trip_id'].nullable = False
fx.schema['TIME_PERIOD'].nullable = False
sensor_zones_df.schema['sensor_zone_id'].nullable = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeTableOut(measurements_df, "Silver_Lakehouse.dbo.OpenAQ_Transformed")
writeTableOut(sensor_df, "Silver_Lakehouse.dbo.dim_sensors")
writeTableOut(parameter_df, "Silver_Lakehouse.dbo.dim_param")
writeTableOut(taxi, "Silver_Lakehouse.dbo.dim_taxi_zones")
writeTableOut(date_df, "Silver_Lakehouse.dbo.dim_dates")
writeTableOut(all_taxi_df, "Silver_Lakehouse.dbo.NYCTaxi_transformed")
writeTableOut(date_df, "Silver_Lakehouse.dbo.dim_dates")
writeTableOut(fx, "Silver_Lakehouse.dbo.dim_fx")
writeTableOut(gdp, "Silver_Lakehouse.dbo.dim_gdp")
writeTableOut(sensor_zones_df, "Silver_Lakehouse.dbo.dim_sensor_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensor_zones_df.schema['sensor_zone_id'].nullable = False
writeTableOut(sensor_zones_df, "Silver_Lakehouse.dbo.dim_sensor_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
