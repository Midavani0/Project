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

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  

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

def writeTableOut(source, dest):
    source.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(dest)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = readTableToDataFrame("Project_Lakehouse.OpenAQ_Raw.OpenAQ_Raw_Combined")
df = replaceDotsInColumnNames(df)
df = dropColumnsByNameSubstrings(df, ['meta', 'flag', 'utc', 'coverage', 'displayName','results_coordinates', 'results_period_label', 'results_coverage_expectedInterval', 'results_coverage_expectedCount', 'results_period_interval', 'results_coverage_observedCount'])
df = replaceSubstringInColumnNames(df, 'results_', '')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeTableOut(df, "Project_Lakehouse.dbo.OpenAQ_Transformed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy("coverage_observedInterval").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy("coverage_percentComplete").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as sf
display(df.select(sf.expr("*")).where(df.coverage_percentComplete == 8.0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select(sf.expr("*")).where(df.parameter_id == 2).orderBy(df.summary_max.desc()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
