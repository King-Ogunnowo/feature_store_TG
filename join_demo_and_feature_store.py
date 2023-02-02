"""
This module enriches the feature store created by the previous script 'create_feature_store.py'

It:
    - reads the feature store data (TEXT_DATA_FEATURE_STORE) and demography data (FINAL_AIRTEL_MTN_DAAS) from snowflake
    - enriches the feature store data with demography data
    - writes the results of this process to s3.
    
Author: Oluwaseyi E. Ogunnowo
"""

# --- importing dependencies
import re
import os
import datetime
import logging
import pyspark
import pyspark.sql.functions as F
import argparse
import pandas as pd
from collections import Counter
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from dependency import utils, cleaning, constants as cnts

# --- creating spark session
conf = SparkConf()\
.set(
    'spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.901,net.snowflake:snowflake-jdbc:3.13.4,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1'
)\
.set(
    'spark.executor.memory','512g'
)\
.set(
    'spark.driver.memory', '512g'
)\
.set(
    "fs.s3a.connection.maximum", 100
)\
.set(
    'spark.sql.shuffle.partitions',300
)
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.master('local[*]').appName('test').getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
spark.sparkContext.setLogLevel("ERROR")

def read_data():
    """
    Function to read the needed data.
    See the docstring above for these required tables
    
    This function is called in the enrich_feature_store() function. See line 109
    
    PARAMETERS
    ----------
    
        This function recieves no parameters
        
    RETURNS
    -------
        
        feature_store: created feature store data
        demography_data: demography data
    
    """
    feature_store = spark.read.format(
        cnts.SNOWFLAKE_SOURCE_NAME
    ).options(
        **cnts.ctx
    ).option(
        "dbtable", "TEXT_DATA_FEATURE_STORE"
    ).load()
    
    demography_data = spark.read.format(
        cnts.SNOWFLAKE_SOURCE_NAME
    ).options(
        **cnts.demography_data_ctx
    ).option(
        "dbtable", "FINAL_AIRTEL_MTN_DAAS"
    ).load()
    
    return feature_store, demography_data

def enrich_feature_store():
    
    """
    This function joins the feature store data to the demography data on msisdns
    The orientation here is a left join, as we want to keep all observations on the left table,
    while getting intersections between the two tables. 
    
    PARAMETERS
    ----------
    
        This function recieves no parameters
        
    RETURNS
    -------
    
        This function does not return any pyspark dataframe objects.
        It writes the results of the join to snowflake
    """
        
    feature_store, demography_data = read_data()
    demography_data.createOrReplaceTempView("demography_data")
    demography_df = spark.sql(cnts.demography_data_query)

    enriched_feature_store = feature_store.join(
        demography_df, 
        demography_df.demo_MSISDN == feature_store.MSISDN,
        "left"
    )

    enriched_feature_store = enriched_feature_store.select(cnts.cols).cache()
    enriched_feature_store.printSchema()
    enriched_feature_store.write.format(
    cnts.SNOWFLAKE_SOURCE_NAME
    ).options(
        **cnts.ctx
        ).option(
            "dbtable", "ENRICHED_TEXT_DATA_FEATURE_STORE"
            ).mode(
                "overwrite"
                ).save()

    
    utils.send_status_msg(cnts.enrichment_success_message)
        
try:
    enrich_feature_store()
except Exception as e:
        utils.send_status_msg(cnts.send_enrichment_failure_message(e))
    