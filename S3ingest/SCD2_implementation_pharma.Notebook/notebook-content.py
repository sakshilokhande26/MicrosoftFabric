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

from pyspark.sql.functions import (
    col, sha2, concat_ws, coalesce, lit, 
    current_date, row_number, max as spark_max, to_timestamp, trim
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# BUILD DIMENSION TABLE 

# Load original file 
original_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("Files/Raw_1/PharmaDrugSales.csv")

# Trim Time to remove any whitespace
original_df = original_df.withColumn("Time", trim(col("Time")))

# Create a Time_Key column (string) for joining
original_df = original_df.withColumn("Time_Key", col("Time"))

# convert Time to timestamp for storage
original_df = original_df.withColumn("Time", to_timestamp(col("Time"), "dd-MM-yyyy HH:mm"))

# Transform other columns
original_df = original_df \
    .withColumn("Year", col("Year").cast("integer")) \
    .withColumn("Month", col("Month").cast("integer")) \
    .withColumn("Date", col("Date").cast("integer")) \
    .withColumn("Hour", col("Hour").cast("integer"))

drug_columns = [
    "AceticAcidDerivatives", "PropionicAcidDerivatives", "SalicylicAcidDerivatives",
    "PyrazolonesAndAnilides", "AnxiolyticDrugs", "HypnoticsSndSedativesDrugs",
    "ObstructiveAirwayDrugs", "Antihistamines"
]

for drug_col in drug_columns:
    original_df = original_df.withColumn(drug_col, col(drug_col).cast("string"))

# Compute hash
hash_columns = [
    "Year", "Month", "Date", "Hour", "Day",
    "AceticAcidDerivatives", "PropionicAcidDerivatives", "SalicylicAcidDerivatives",
    "PyrazolonesAndAnilides", "AnxiolyticDrugs", "HypnoticsSndSedativesDrugs",
    "ObstructiveAirwayDrugs", "Antihistamines"
]

original_df = original_df.withColumn(
    "row_hash",
    sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in hash_columns]), 256)
)

display(original_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add SCD2 metadata
window_spec = Window.orderBy("Time_Key")
original_df = original_df \
    .withColumn("surrogate_key", row_number().over(window_spec)) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit("9999-12-31").cast("date")) \
    .withColumn("is_current", lit(1))

# Reorder columns (include Time_Key)
columns_order = [
    "surrogate_key", "Time", "Time_Key", "Year", "Month", "Date", "Hour", "Day",
    "AceticAcidDerivatives", "PropionicAcidDerivatives", 
    "SalicylicAcidDerivatives", "PyrazolonesAndAnilides",
    "AnxiolyticDrugs", "HypnoticsSndSedativesDrugs",
    "ObstructiveAirwayDrugs", "Antihistamines",
    "row_hash", "start_date", "end_date", "is_current"
]

original_df = original_df.select(columns_order)

# Save dimension table
spark.sql("DROP TABLE IF EXISTS df_scd2")
original_df.write.format("delta").mode("overwrite").saveAsTable("df_scd2")

print(f"Dimension table created with {original_df.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# LOAD AND TRANSFORM SOURCE
#"LOADING MODIFIED SOURCE FILE"


source_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("Files/Raw_1/PharmaDrugSales_SCD2.csv")

# Trim Time and create Time_Key
source_df = source_df.withColumn("Time", trim(col("Time")))
source_df = source_df.withColumn("Time_Key", col("Time"))

# Convert Time to timestamp
source_df = source_df.withColumn("Time", to_timestamp(col("Time"), "dd-MM-yyyy HH:mm"))

# Apply SAME transformations
source_df = source_df \
    .withColumn("Year", col("Year").cast("integer")) \
    .withColumn("Month", col("Month").cast("integer")) \
    .withColumn("Date", col("Date").cast("integer")) \
    .withColumn("Hour", col("Hour").cast("integer"))

for drug_col in drug_columns:
    source_df = source_df.withColumn(drug_col, col(drug_col).cast("string"))

source_df = source_df.withColumn(
    "row_hash",
    sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in hash_columns]), 256)
)

print(f"Source loaded with {source_df.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# IDENTIFY CHANGES USING Time_Key
# IDENTIFYING NEW, CHANGED, UNCHANGED RECORDS
# source_df = PharmaDrugSales_SCD2 (modified table)
# current_dim_df = df_scd2 (old table)

# Load dimension
delta_table = DeltaTable.forName(spark, "df_scd2")
dim_df = delta_table.toDF()
current_dim_df = dim_df.filter(col("is_current") == 1)

# JOIN USING Time_Key (STRING) INSTEAD OF Time (TIMESTAMP) ***
joined_df = source_df.alias("src").join(
    current_dim_df.alias("dim"),
    col("src.Time_Key") == col("dim.Time_Key"),
    "left"
)

# Identify record types
new_records = joined_df.filter(col("dim.Time_Key").isNull()).select("src.*")
changed_records = joined_df.filter(
    (col("dim.Time_Key").isNotNull()) & 
    (col("src.row_hash") != col("dim.row_hash"))
).select("src.*")
unchanged_records = joined_df.filter(
    (col("dim.Time_Key").isNotNull()) & 
    (col("src.row_hash") == col("dim.row_hash"))
)

new_count = new_records.count()
changed_count = changed_records.count()
unchanged_count = unchanged_records.count()

print("SCD2 RECORD ANALYSIS")
print(f"New Records:       {new_count}")
print(f"Changed Records:   {changed_count}")
print(f"Unchanged Records: {unchanged_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXPIRE OLD VERSIONS
# EXPIRING OLD VERSIONS OF CHANGED RECORDS


if changed_count > 0:
    # Get Time_Key values of changed records
    changed_keys = changed_records.select("Time_Key")
    
    # Find records to expire
    records_to_expire = current_dim_df.alias("dim").join(
        changed_keys.alias("chg"),
        col("dim.Time_Key") == col("chg.Time_Key"),
        "inner"
    ).select(
        col("dim.surrogate_key"),
        current_date().alias("new_end_date"),
        lit(0).alias("new_is_current")
    )
    # Perform MERGE to update
    delta_table.alias("target").merge(
        records_to_expire.alias("source"),
        "target.surrogate_key = source.surrogate_key"
    ).whenMatchedUpdate(set={
        "end_date": col("source.new_end_date"),
        "is_current": col("source.new_is_current")
    }).execute()
    
    print(f"Expired {records_to_expire.count()} old record versions")
else:
    print("No records to expire.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT NEW RECORDS
# INSERTING NEW RECORDS AND NEW VERSIONS


# Refresh dimension after update
dim_df = spark.table("df_scd2")
max_sk = dim_df.agg(spark_max("surrogate_key")).collect()[0][0] or 0
print(f"Current max surrogate key: {max_sk}")

# Prepare new records
records_to_insert = None
window_spec = Window.orderBy("Time_Key")

if new_count > 0:
    new_records_prepared = new_records \
        .withColumn("surrogate_key", row_number().over(window_spec) + max_sk) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31").cast("date")) \
        .withColumn("is_current", lit(1))
    
    records_to_insert = new_records_prepared
    max_sk = max_sk + new_count
    print(f"Prepared {new_count} new records")

# Prepare changed records (new versions)
if changed_count > 0:
    changed_records_prepared = changed_records \
        .withColumn("surrogate_key", row_number().over(window_spec) + max_sk) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31").cast("date")) \
        .withColumn("is_current", lit(1))
    
    if records_to_insert is not None:
        records_to_insert = records_to_insert.union(changed_records_prepared)
    else:
        records_to_insert = changed_records_prepared
    
    print(f"Prepared {changed_count} new versions of changed records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Insert all records
if records_to_insert is not None:
    records_to_insert_final = records_to_insert.select(columns_order)
    
    count_before = spark.table("df_scd2").count()
    
    records_to_insert_final.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("df_scd2")
    
    count_after = spark.table("df_scd2").count()
    
  
    print(f"Records before:        {count_before}")
    print(f"Records after:         {count_after}")
    print(f"New records inserted:  {new_count}")
    print(f"Changed versions:      {changed_count}")
    print(f"Total inserted:        {count_after - count_before}")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
