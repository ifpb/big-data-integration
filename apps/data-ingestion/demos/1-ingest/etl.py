# import libraries
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

import pyspark.sql.functions as func
import findspark

findspark.init()

# data processing using a data lake
# blob storage = azure
# s3 = amazon
# gcs = google
# minio = k8s
# hdfs = hadoop

load_dotenv()
MINIO = os.getenv("MINIO")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

dir = os.path.join(
    "/Users/diegopessoa/projects/ifpb/integracao-dados/big-data-integration/apps/data-ingestion/demos/1-ingest",
    "jars")
jars = [os.path.join(dir, x) for x in os.listdir(dir)]

# set config
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO}")
    .set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.connection.maximum", 100)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .set("spark.jars", ",".join(jars)))


# apply config
sc = SparkContext(conf=conf).getOrCreate()

# main spark program
if __name__ == '__main__':


    # init spark session
    # name of the app
    spark = SparkSession \
            .builder \
            .appName("etl-idados") \
            .getOrCreate()

    # set log level to info
    spark.sparkContext.setLogLevel("INFO")

    # read data frame [json]
    # get data from processing zone
    # list all files available
    df_vehicles = spark.read \
        .option("inferSchema", "true") \
        .json("s3a://landing/vehicle/*.json", multiLine=True)

    # display data into dataframe
    df_vehicles.show()

    # print schema
    df_vehicles.printSchema()

    # register df into sql engine
    df_business.createOrReplaceTempView("vw_vehicle")

    # use sql engine to query data
    # select important columns
    df_business_sql = spark.sql("SELECT id, vin, user_id, color FROM vw_vehicle")

    # display sql data frame
    df_business_sql.show()

    # save into d


    # stop spark session
    spark.stop()
