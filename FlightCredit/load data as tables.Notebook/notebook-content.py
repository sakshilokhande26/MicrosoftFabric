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

# Store Flight Booking data partitioned by month

from pyspark.sql.functions import year, month, col, to_timestamp, to_date

Flight_input_path = "Files/input/flights/"
credit_input_path = "Files/input/credit/"

df_flights = spark.read.option("multiline","true").json(Flight_input_path + "*.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_flights = df_flights\
.withColumn("ArrivalTimestamp",to_timestamp(col("ArrivalTimestamp"),"yyyy-MM-dd HH:mm:ss"))\
.withColumn("DepartTimestamp",to_timestamp(col("DepartTimestamp"),"yyyy-MM-dd HH:mm:ss"))


df_flights_partitioned = df_flights.withColumn("fly_year",year(col("DepartTimestamp")))\
.withColumn("fly_month",month(col("DepartTimestamp")))

df_flights_partitioned.write.mode("append").partitionBy("fly_year","fly_month")\
.format("delta").saveAsTable("flight_bookings_partitioned")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load credit card details table

df_creditdetails = spark.read.option("multiline","true").json(credit_input_path + "*.json")

df_creditdetails = df_creditdetails.withColumn("transactiondate",to_date(col("transactiondate"),"yyyy-MM-dd"))

df_creditdetails.write.mode("append").format("delta")\
.saveAsTable("credit_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
