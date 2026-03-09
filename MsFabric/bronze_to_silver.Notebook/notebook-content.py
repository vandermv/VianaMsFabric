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

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_root = "Files/bronze/adventureworks"
silver_schema = "silver"
watermark_table = "metadata.watermark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = [
    f.name.lower() for f in mssparkutils.fs.ls(bronze_root)
    if f.isDir
]

print(tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.watermark (
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

tables = [
    f.name for f in mssparkutils.fs.ls(bronze_root)
    if f.isDir
]

for table in tables:

    print(f"Processing {table}")

    bronze_path = f"{bronze_root}/{table}"

    df = spark.read.format("parquet") \
        .option("recursiveFileLookup","true") \
        .load(bronze_path)

    df = df.withColumn("load_timestamp", current_timestamp())

    silver_table = f"{silver_schema}.{table.lower()}"

    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema","true") \
        .saveAsTable(silver_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
