from pyspark.sql.functions import col

def clean_customer(df):

    df = df.filter(col("customer_id").isNotNull())

    df = df.fillna({
        "address": "UNKNOWN"
    })

    return df