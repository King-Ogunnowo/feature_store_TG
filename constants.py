import datetime
from dependency import utils


ctx = {
      'sfURL':'ps98646.eu-west-1.snowflakecomputing.com',
      'sfUser':"OLUWASEYI",
      'sfAccount':'ps98646.eu-west-1',
      'sfPassword':"Emma1095!",
      'sfWarehouse':'ANALYTICS',
      'sfDatabase':'DATATEAM_ML_DB',
      'sfRole':'REPORTER',
      'sfSchema':'AUDIENCE_PLATFORM_MODELS_SCH'}

demography_data_ctx = {
          'sfURL':'ps98646.eu-west-1.snowflakecomputing.com',
          'sfUser':"OLUWASEYI",
          'sfAccount':'ps98646.eu-west-1',
          'sfPassword':"Emma1095!",
          'sfWarehouse':'ANALYTICS',
          'sfDatabase':'WH_DB',
          'sfRole':'REPORTER',
          'sfSchema':'DEMOGRAPHY'}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

demography_data_query =  """select MSISDN as demo_MSISDN,
                                    AGE, AGE_GROUP, OPERATOR, GENDER, CUSTOMERCLASS,
                                    LOCATION_REGION, LOCATION_STATE,
                                    LOCATION_LGA, ACTIVE_DATA_USER,
                                    DEVICE_TYPE, YEAR_OF_BIRTH,
                                    DATE_OF_BIRTH
                                    from demography_data
                                    """

cols = ["MSISDN", "AGE", "AGE_GROUP", "OPERATOR", "GENDER", 
            "CUSTOMERCLASS", "LOCATION_REGION", "LOCATION_STATE",
            "LOCATION_LGA", "ACTIVE_DATA_USER", "DEVICE_TYPE",
            "YEAR_OF_BIRTH", "DATE_OF_BIRTH", "TYPE_CLEAN",
            "MESSAGES", "CLEAN_MESSAGES", "TRANSACTION_CATEGORY", 
            "POS", "CHARGES", "TRANSFER", "AIRTIME_DATA", "WEB", 
            "MONTH", "YEAR", "TIMESTAMP"]

month, year = utils.get_previous_month()
month_name = datetime.datetime.strptime(str(month), '%m').strftime('%B')

ner_query = "select distinct msisdn, type, clean_msg as messages from ner_data"  

data_already_present_notification_dict = {
'username':'Extraction Pipeline Notification',
'text':f"Hello <@U020H5E9VRC>, :warning: You cannot update feature store with text data of month {month_name}, that data is already present in the feature store."
}

success_update_notification_dict = {
'username':'Extraction Pipeline Notification',
'text':f"Hello <@U020H5E9VRC>, :bananadance: I have '**updated**' the previous results in the feature store with text data for {month_name}, {year}. Please note that I did not delete the data for the previous month, since we are working on a 2-month sliding window :smile:. You can find my results here: select * from DATATEAM_ML_DB.AUDIENCE_PLATFORM_MODELS_SCH.TEXT_DATA_FEATURE_STORE limit 10"
}

success_overwrite_notification_dict = {
    'username':'Extraction Pipeline Notification',
'text':f"Hello <@U020H5E9VRC>, :bananadance: I have '**overwritten**' the previous results in the feature store with text data for {month_name}, {year}. Please note that I deleted the previously existing data in the feature store, since we are working on a 2-month sliding window :smile:. You can find my results here: select * from DATATEAM_ML_DB.AUDIENCE_PLATFORM_MODELS_SCH.TEXT_DATA_FEATURE_STORE limit 10"
}

enrichment_success_message = {
    'username':'Extraction Pipeline Notification',
    'text':f"Hello <@U020H5E9VRC>, :bananadance: I have enriched profiles in the feature store with demographic data. You can find my results here: select * from {ctx['sfDatabase']}.{ctx['sfSchema']}.ENRICHED_TEXT_DATA_FEATURE_STORE limit 10"
}
    


def send_failure_notification(e):
    failure_notification_dict = {'username':'Extraction Pipeline Notification',
                                 'text':f"Hello <@U020H5E9VRC>, :cry: I failed at updating the feature store with text data for {month_name}, {year}. Reason: {e}"}
    return failure_notification_dict

def send_enrichment_failure_message(e):
    enrichment_failure_message = {
        'username':'Extraction Pipeline Notification',
        'text':f"Hello <@U020H5E9VRC>, :cry: I failed at enriching the feature store with demographic data. I encountered this problem: {e}"
    }
    return enrichment_failure_message
    




