from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType
import os


def apply_scd2(spark, source_df, target_path):

    df = source_df.withColumn("start_date", current_timestamp()) \
                  .withColumn("end_date", lit(None).cast(TimestampType())) \
                  .withColumn("is_current", lit(True))

    # Check if target path exists
    csv_file = target_path + ".csv"
    if os.path.exists(csv_file):
        print(f"CSV table exists at {csv_file} → reading and appending")
        
        existing_df = spark.read.option("header", "true").csv(csv_file)
        
        # For SCD2, mark old records as inactive and append new ones
        updated_existing = existing_df.withColumn("is_current", lit(False))
        
        result_df = updated_existing.union(df)
        result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_file)
    else:
        print(f"Creating new CSV table at {csv_file}")
        df.coalesce(1).write.option("header", "true").csv(csv_file)