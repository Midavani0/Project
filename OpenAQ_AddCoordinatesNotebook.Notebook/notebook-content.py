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
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

input_json = r""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
input_json = json.JSONDecoder().decode(input_json)
result = dict()
for i in input_json['results']:
    key = str(i['coordinates']['latitude']) +' '+ str(i['coordinates']['longitude'])
    result[key] = ''
    for j in i["sensors"]:
        result[key] = str(result[key]) + ' ' + str(j['id'])

for i in result.keys():
    result[i] = result[i].lstrip().split(' ')
print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
from pyspark.sql import functions as sf

tables = spark.catalog.listTables("OpenAQ_Raw")

for table in tables:
    for i in result.keys():
        if table.name.replace("raw_", "") in result[i]:
            print(table.name)
            df = spark.read.synapsesql(f"Project_Lakehouse.OpenAQ_Raw.{table.name}")
            df = df.withColumn('coordinates', sf.lit(i))
            df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"Project_Lakehouse.OpenAQ_Raw.{table.name}")

            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
