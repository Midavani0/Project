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
