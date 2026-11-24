# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

ids = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ids = ids[ids.index('result'):ids.index('ADFHttpStatusCodeInResponse')]
ids = ids.split('locality')
result = []
for i in range(1, len(ids)):
    ids[i] = ids[i][ids[i].index('sensors'):]
    ids[i] = ids[i][:ids[i].index(']')]
    ids[i] = ids[i].split('},')
    for j in range(len(ids[i])):
        ids[i][j] = ids[i][j][ids[i][j].index('"id":') + 5:ids[i][j].index(',')]
        result.append(ids[i][j])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from pyspark.sql.types import StructType

directory = os.fsencode("/lakehouse/default/Files/")

def replaceDotsInColumnNames(df):
    names = df.schema.names
    new_names = dict()
    for name in names:
        new_names[name] = name.replace('.', '_')
    return df.withColumnsRenamed(new_names)

default_schema = replaceDotsInColumnNames(spark.read.parquet("Files/10454036/028d29f9-9f49-49e6-85d7-a281a08c3274.parquet")).schema

for foulder in os.listdir(directory):
    df = spark.createDataFrame([], StructType(default_schema))
    for file in os.listdir(directory.decode("utf-8") + foulder.decode("utf-8")):
        if file != '_delta_log':
            temp = spark.read.parquet("Files/" + foulder.decode("utf-8") + '/' + file)
            temp = replaceDotsInColumnNames(temp)
            temp_schema = temp.schema.names
            if temp_schema != default_schema.names:
                continue
            df = df.join(temp,temp_schema, "full")
    print(foulder)
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"Project_Lakehouse.OpenAQ_Raw.Raw_{foulder.decode("utf-8")}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
