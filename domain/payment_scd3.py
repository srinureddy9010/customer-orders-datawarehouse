from pyspark.sql.functions import col

def apply_scd3(df):

    df = df.withColumnRenamed("method", "current_method")

    df = df.withColumn("previous_method", col("current_method"))

    return df