"""
This module creates the text data feature store by:
    - reading the NER predictions data
    - tokenizing all texts per msisdn
    - Categorizing each text according to transaction channels
    
The feature store operates on a two month sliding window.
If the current feature store data is just one month worth, it is updated by adding a second month
If it is two months worth, the data is overwritten and a single month worth of data is uploaded. 

For example:
    - One Month Scenario:
        January feature store data is present, February is added.
        
    - Two month scenario:
        January and February feature store data are present, they are overwritten and replaced with March

Author: Oluwaseyi E. Ogunnowo
"""

# --- importing dependencies
import re
import os
import datetime
import boto3
import logging
import pyspark
import pyspark.sql.functions as F
import argparse
import pandas as pd
from collections import Counter
from io import BytesIO
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from dependency import utils, cleaning,  constants as cnts

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
    
    month, year = cnts.month, cnts.year
    
    NER_path = f's3a://datateam-ml/Insights_Extraction/Ner/YEAR={year}/MONTH={month}/'
    feature_store_table = "TEXT_DATA_FEATURE_STORE"
    
    ner_data = spark.read.format('parquet').option("inferSchema",
                                                                "true").load(NER_path)
    feature_store_data = spark.read.format(
        cnts.SNOWFLAKE_SOURCE_NAME
    ).options(
        **cnts.ctx
    ).option(
        "dbtable", "TEXT_DATA_FEATURE_STORE"
    ).load()
    
    return ner_data, feature_store_data


# --- creating keywords dictionary for feature store
def create_keyword_dictionary():
    """
    This function creates a dictionary object containing categories and keywords
    
     PARAMETERS
    ----------
    
        This function recieves no parameters
        
    RETURNS
    -------
        
        transaction_category: A dictionary object 
    """
    s3 = boto3.client('s3')
    object_ = s3.get_object(
        Bucket='datateam-ml', 
        Key='text_data_feature_store/bank_keywords/bank_key_words.xlsx'
    )
    binary_data = object_['Body'].read()
    
    keywords_sheet_names = utils.get_sheetnames_xlsx(BytesIO(binary_data))
    transaction_category = utils.merge_sheet_observations(
        *tuple([
            utils.keywords_extractor(pd.read_excel(
                binary_data,
                sheet_name=sheet_name
            ))
            for sheet_name in keywords_sheet_names
        ])
    )
    return transaction_category

def update_feature_store(ner_data, feature_store_data, previous_month, ner_query):
    
    """
    This function updates the feature store on the condition that it contains just one month worth of data
    See illustration above in module string
    
    PARAMETERS
    ----------
    
        ner_data: NER Predictions data
        feature_store_data: Feature store data of the previous month
        previous_month: Previous month
        ner_query: query to collect necessary data from the NER table. see constants module in dependency folder
        
    RETURNS
    -------
    
        Does not return any object, results are written into snowflake
    """

    month, year = cnts.month, cnts.year
    timestamp = datetime.datetime.now()
    transaction_category = create_keyword_dictionary()
    clean_text = udf(lambda x: cleaning.clean_text(x))
    clean_trans_type = udf(lambda x: cleaning.clean_type(x))
    udf_detectmessage = udf(lambda x: cleaning.get_search_group(x))
    identify_categories = udf(lambda x: utils.get_categories(x, transaction_category))
    frequency = udf(lambda s: dict(Counter(s)), MapType(StringType(), IntegerType()))

    ner_data.createOrReplaceTempView("ner_data")
    ner_df = spark.sql(ner_query)

    previous_month = feature_store_data.first().MONTH

    if month == previous_month:
        utils.send_status_msg(

        )
        exit()
    else:
        ner_df = ner_df.withColumn("type_clean", clean_trans_type(lower(col("TYPE"))))

        updated_feature_store = feature_store_data.select(
            "msisdn", "type_clean", "messages"
        ).union(
            ner_df.select(
                "msisdn","type_clean","messages")
        )

    updated_feature_store = updated_feature_store.groupBy(
        "msisdn", "type_clean"
    ).agg(concat_ws(
        ', ', collect_list("messages")
    ).alias(
        "messages")
         )

    updated_feature_store = updated_feature_store.withColumn("clean_messages", clean_text("messages"))\
          .withColumn("transaction_category", frequency(identify_categories("clean_messages")))\
          .withColumn("POS", F.col("transaction_category").getItem("POS"))\
          .withColumn("Charges", F.col("transaction_category").getItem("Charges"))\
          .withColumn("Transfer", F.col("transaction_category").getItem("Transfer"))\
          .withColumn("Airtime_Data", F.col("transaction_category").getItem("Airtime_Data"))\
          .withColumn("WEB", F.col("transaction_category").getItem("WEB"))\
          .withColumn("month", F.expr(f'array({previous_month}, {month})'))\
          .withColumn("year", lit(str(year)))\
          .withColumn("timestamp", lit(timestamp))
    updated_feature_store = updated_feature_store.cache()
    updated_feature_store = updated_feature_store.na.fill(value=0)
    updated_feature_store.write.format(
        cnts.SNOWFLAKE_SOURCE_NAME
        ).options(
            **cnts.ctx
            ).option(
                "dbtable", "TEXT_DATA_FEATURE_STORE"
                ).mode(
                    "overwrite"
                    ).save()
    
    utils.send_status_msg(
        cnts.success_update_notification_dict
        )
        
def overwrite_feature_store(ner_data, feature_store_data, previous_month, ner_query):
    """
    This function overwrites the feature store on the condition that it contains two months worth of data.
    See illustration above in module string
    
    PARAMETERS
    ----------
    
        ner_data: NER Predictions data
        feature_store_data: Feature store data of the previous month
        previous_month: Previous month
        ner_query: query to collect necessary data from the NER table. see constants module in dependency folder
        
    RETURNS
    -------
    
        Does not return any object, results are written into snowflake
    """
    
    month, year = cnts.month, cnts.year
    timestamp = datetime.datetime.now()
    transaction_category = create_keyword_dictionary()
    clean_text = udf(lambda x: cleaning.clean_text(x))
    clean_trans_type = udf(lambda x: cleaning.clean_type(x))
    udf_detectmessage = udf(lambda x: cleaning.get_search_group(x))
    identify_categories = udf(lambda x: utils.get_categories(x, transaction_category))
    frequency = udf(lambda s: dict(Counter(s)), MapType(StringType(), IntegerType()))

    if str(month) in previous_month:
        utils.send_status_msg(
            {
                'username':'Extraction Pipeline Notification',
                'text':f"Hello <@U020H5E9VRC>, :warning: You cannot update feature store with text data of month {month_name}, that data is already present in the feature store."
            }
        )
        exit()
    else:
        ner_data.createOrReplaceTempView("ner_data")
        ner_df = spark.sql(ner_query)

        current_feature_store = ner_df.withColumn("type_clean", clean_trans_type(lower(col("TYPE"))))
        current_feature_store = current_feature_store.withColumn("clean_messages", clean_text("messages"))\
              .withColumn("transaction_category", frequency(identify_categories("clean_messages")))\
              .withColumn("POS", F.col("transaction_category").getItem("POS"))\
              .withColumn("Charges", F.col("transaction_category").getItem("Charges"))\
              .withColumn("Transfer", F.col("transaction_category").getItem("Transfer"))\
              .withColumn("Airtime_Data", F.col("transaction_category").getItem("Airtime_Data"))\
              .withColumn("WEB", F.col("transaction_category").getItem("WEB"))\
              .withColumn("month", lit(month))\
              .withColumn("year", lit(str(year)))\
              .withColumn("timestamp", lit(timestamp))
        current_feature_store = current_feature_store.cache()
        current_feature_store = current_feature_store.na.fill(value=0)
        current_feature_store.write.format(
        cnts.SNOWFLAKE_SOURCE_NAME
        ).options(
            **cnts.ctx
            ).option(
                "dbtable", "TEXT_DATA_FEATURE_STORE"
                ).mode(
                    "overwrite"
                    ).save()
        
        utils.send_status_msg(
            cnts.success_overwrite_notification_dict
            )


def handler_function():
    
    """
    Ties all of the functions together in a single function, so it is easy to call with the approprate conditions
    """
    
    ner_data, feature_store_data = read_data()
    previous_month = feature_store_data.first().MONTH
    
    if len(str(previous_month)) == 1:
        update_feature_store(ner_data, 
                                      feature_store_data, 
                                      previous_month, 
                                      cnts.ner_query)
                                     
    if len(str(previous_month)) > 1:
        overwrite_feature_store(ner_data, 
                                         feature_store_data,
                                         previous_month, 
                                         cnts.ner_query)
                                         
            
try:
    handler_function()
except Exception as e:
    utils.send_status_msg(cnts.send_failure_notification(e))
