import os

def apply_scd1(spark, source_df, target_path):

    print("Running SCD1 merge for product dimension")

    csv_file = target_path + ".csv"
    if os.path.exists(csv_file):

        print("CSV table exists → running merge")

        existing_df = spark.read.option("header", "true").csv(csv_file)
        
        # For SCD1, simply overwrite with new data
        source_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_file)

    else:

        print("Creating new CSV table at:", csv_file)

        source_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(csv_file)

        print("CSV table created successfully")