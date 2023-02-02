"""
Module to create spark session
"""
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# --- creating spark session
conf = SparkConf()\
.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.901,net.snowflake:snowflake-jdbc:3.13.4,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1')\
.set('spark.executor.memory','512g')\
.set('spark.driver.memory', '512g')\
.set("fs.s3a.connection.maximum", 100)\
.set('spark.sql.shuffle.partitions',300)
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.master('local[*]').appName('test').getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
spark.sparkContext.setLogLevel("ERROR")
