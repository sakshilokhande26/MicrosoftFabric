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

df_flights = spark.table("flight_bookings_partitioned")
df_credit = spark.table("credit_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_flights)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, quarter

df_flights_filtered = df_flights.filter((col("FlightStatus")=="Flown")\
 & (col('TransactionType')=="Credit") & (col('PassengerFlown')=="Yes") \
 & (col('fly_year')==2024) & (quarter(col('DepartTimestamp'))==3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flights_filtered.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flights_filter = df_flights_filtered.filter(col('NumberOfSeats')*col('Price') > 100000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_flights_filter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_extracted = df_flights_filter.join\
(df_credit, df_flights_filtered['TransactionID']==df_credit['transactionID'], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_extracted)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_extracted.select('PassengerID','PassengerName',\
'bankname', 'merchant','partner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.write.mode("overwrite").format("delta").saveAsTable("Q3rewards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_file_path = "Files/output/q3_reward_cust_list.csv"

df_final.write.mode("overwrite").format("csv").option("header","true").save(csv_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
