# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "051ef925-6a2d-4fc4-8d99-33b34927d133",
# META       "default_lakehouse_name": "FlightCreditLH",
# META       "default_lakehouse_workspace_id": "286e3ef4-2f4b-4f1e-88a2-382c278612ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "051ef925-6a2d-4fc4-8d99-33b34927d133"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Archive Files after loading

from datetime import datetime

archive_path = "Files/archive/"
timestamp1 = datetime.now().strftime("%Y%m%d_%H%M%S")

Flight_input_path = "Files/input/flights/"
credit_input_path = "Files/input/credit/" 

flight_files = mssparkutils.fs.ls(Flight_input_path)
json_flight_files = [f.path for f in flight_files if f.path.endswith('.json')]

credit_files = mssparkutils.fs.ls(credit_input_path)
json_credit_files = [f.path for f in credit_files if f.path.endswith('.json')]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in json_flight_files:
    filename = file.split('/')[-1]
    archive_dest = f"{archive_path}flights/{timestamp1}_{filename}"
    mssparkutils.fs.mv(file,archive_dest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for files in json_credit_files:
    filesname = files.split('/')[-1]
    archive = f"{archive_path}credit/{timestamp1}_{filesname}"
    mssparkutils.fs.mv(files,archive)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
