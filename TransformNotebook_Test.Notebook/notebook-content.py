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
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "f11bf2e2-171c-49b3-a231-5ee227fdb881",
# META       "known_warehouses": [
# META         {
# META           "id": "f11bf2e2-171c-49b3-a231-5ee227fdb881",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
from pyspark.sql import functions as sf

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

def dropColumnsByNameSubstrings(df, columns = []):
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

df = df.join(roundValuesOfColumnsTo(spark.read.synapsesql('Project_Lakehouse.dbo.locationsTable'), ['coordinates_latitude', 'coordinates_longitude'], 12), on=['coordinates_latitude', 'coordinates_longitude'],how = 'full')
#df = df.join(spark.read.synapsesql('Project_Lakehouse.dbo.sensor_locations_table'), on=['coordinates_latitude', 'coordinates_longitude'],how = 'full')
df = df.join(roundValuesOfColumnsTo(spark.read.synapsesql('Project_Lakehouse.dbo.broader_sensors_location_table'), ['coordinates_latitude', 'coordinates_longitude'], 12), on=['coordinates_longitude', 'coordinates_latitude'],how = 'full')

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

writeTableOut(df, "Silver_Lakehouse.dbo.OpenAQ_Transformed")


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
display(taxi)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.join(taxi, 'sensor_zone_id').select("sensor_zone_name", "parameter_name", 'value', 'Borough').where(df["parameter_name"] == 'um003').orderBy("value", ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
