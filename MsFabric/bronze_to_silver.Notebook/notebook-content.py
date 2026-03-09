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

# Import required Spark and Delta Lake libraries

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Bronze and Silver locations and watermark table

bronze_root = "Files/bronze/adventureworks"
silver_schema = "silver"
watermark_table = "metadata.watermark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Automatically discover all Bronze tables by scanning the filesystem

tables = [
    f.name for f in mssparkutils.fs.ls(bronze_root)
    if f.isDir
]

print("Discovered tables:", tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create metadata schema and watermark control table if they do not exist

spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {watermark_table} (
    table_name STRING,
    last_load TIMESTAMP
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to retrieve the last processed timestamp (watermark)

def get_watermark(table):

    df = spark.sql(f"""
        SELECT last_load
        FROM {watermark_table}
        WHERE table_name = '{table}'
    """)

    result = df.collect()

    if len(result) == 0:
        return None
    
    return result[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to update watermark after successful processing

def update_watermark(table):

    spark.sql(f"""
    MERGE INTO {watermark_table} t
    USING (
        SELECT '{table}' as table_name,
               current_timestamp() as last_load
    ) s
    ON t.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET last_load = s.last_load
    WHEN NOT MATCHED THEN INSERT *
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process Bronze tables and load them incrementally into Silver using Delta MERGE
# This step reads Bronze data, removes duplicates, and applies CDC logic

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

for table in tables:

    print(f"Processing table: {table}")

    bronze_path = f"{bronze_root}/{table}"
    silver_table = f"{silver_schema}.{table.lower()}"

    # Read Bronze data recursively to include all partitions
    df = spark.read.format("parquet") \
        .option("recursiveFileLookup","true") \
        .load(bronze_path)

    # Add metadata column to track ingestion time
    df = df.withColumn("load_timestamp", current_timestamp())

    # Identify primary key (temporary strategy: first column)
    key = df.columns[0]

    # Deduplicate source records to prevent multiple matches during MERGE
    window = Window.partitionBy(key).orderBy(col("load_timestamp").desc())

    df = df.withColumn(
        "rn",
        row_number().over(window)
    ).filter("rn = 1").drop("rn")

    # If Silver table does not exist, create it
    if not spark.catalog.tableExists(silver_table):

        print("Creating new Silver table")

        df.write.format("delta") \
          .mode("overwrite") \
          .option("mergeSchema","true") \
          .saveAsTable(silver_table)

    else:

        print("Running CDC MERGE")

        deltaTable = DeltaTable.forName(spark, silver_table)

        (
            deltaTable.alias("target")
            .merge(
                df.alias("source"),
                f"target.{key} = source.{key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Update watermark after successful load
    update_watermark(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optimize Delta tables to improve query performance

for table in tables:

    spark.sql(f"OPTIMIZE {silver_schema}.{table.lower()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
