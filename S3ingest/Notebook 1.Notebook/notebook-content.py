# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee0b913e-dd5f-44fb-8a2e-ff524839691e",
# META       "default_lakehouse_name": "POC_lakehouse",
# META       "default_lakehouse_workspace_id": "245a7b82-f6cb-47fc-801f-ca03bcf204ef",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee0b913e-dd5f-44fb-8a2e-ff524839691e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.format("parquet").load("Files/Raw/PharmaDrugSales")

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

df.write.mode("overwrite").format("delta").saveAsTable("PharmaDrugSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = df.count()
cols = len(df.columns)

print(f"Rows: {rows}, Columns: {cols}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
