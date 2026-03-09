# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00879f07-66d5-42fe-bbae-456d901cf25a",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "2fe7779c-0c8d-45e9-838f-2f2b85ff724c",
# META       "known_lakehouses": [
# META         {
# META           "id": "00879f07-66d5-42fe-bbae-456d901cf25a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable

table_name = "silver.address"

# Check if the table already exists
if spark.catalog.tableExists(table_name):

    deltaTable = DeltaTable.forName(spark, table_name)

    (
    deltaTable.alias("silver")
    .merge(
        df_clean.alias("updates"),
        "silver.AddressID = updates.AddressID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )

else:
    
    # Create the table if it does not exist
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import input_file_name, current_timestamp, when, col

def ingest_to_silver(source_path, table_name, business_key):

    # Read source files
    df = spark.read.format("csv") \
        .option("header","true") \
        .load(source_path)

    # Add metadata columns
    df = (
        df.withColumn("FileName", input_file_name())
          .withColumn("CreatedTS", current_timestamp())
          .withColumn("ModifiedTS", current_timestamp())
          .withColumn("IsFlagged",
              when(col(business_key).isNull(), 1).otherwise(0)
          )
    )

    # Remove duplicates
    df_clean = df.dropDuplicates([business_key])

    # If table exists -> MERGE
    if spark.catalog.tableExists(table_name):

        deltaTable = DeltaTable.forName(spark, table_name)

        (
        deltaTable.alias("target")
        .merge(
            df_clean.alias("source"),
            f"target.{business_key} = source.{business_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        )

    else:
        # Create table
        df_clean.write.format("delta").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
