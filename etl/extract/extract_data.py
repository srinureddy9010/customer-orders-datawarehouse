import os
from pyspark.sql import SparkSession

def create_spark():

    # Set Hadoop home properly
    if 'HADOOP_HOME' not in os.environ:
        os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    
    os.environ['HADOOP_OPTS'] = '-Djava.security.auth.login.config=ignore'

    spark = (
        SparkSession.builder
        .appName("CustomerOrdersDW")
        .master("local[*]")
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.hadoop.security.authorization", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    return spark


def read_json(spark, path):

    df = spark.read.option("multiline", "true").json(path)

    return df