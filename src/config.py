from datetime import datetime

today = datetime.now()
start_time = today.strftime("%Y-%m-%d_%H:%M:%S")

#Snowflake Credentials
ACCOUNT = 'placeholder for your account'
USERNAME = 'placeholder for user name'
PASSWORD = 'placeholder for password'
DATABASE = 'placeholder for database'
SCHEMA = 'placeholder for schema'
WAREHOUSE= 'placeholder for warehouse'
ROLE = 'placeholder for for role'

#table details
Table_Names = ['list' , 'of', 'Tables' ]

#AWS Client Setup Credentials
AccessKey = 'placeholder for access key'
SecretKey = 'placeholder for secret key'

#Feed Export
FEED_EXPORT_CONFIG = {
    "unload_snowflake" : {
        "file_format": "create or replace file format unload_snowflake_Report_{DATE_TIME} COMPRESSION = GZIP FILE_EXTENSION = 'csv' type = 'CSV' NULL_IF=() FIELD_OPTIONALLY_ENCLOSED_BY = '\"'; ",
        "stage_template": "create or replace stage unload_snowflake_{DATE_TIME} " + "url='" +
                          "s3:======Your s3 path ================/{TABLE}/" + "' " + 
                          "credentials=(aws_key_id='{ACCESS_KEY}' aws_secret_key='{SECRET_KEY}') "
                          "encryption=(type='AWS_SSE_S3') "
                          "file_format = unload_snowflake_Report_{DATE_TIME};",
        "table_name":"""show columns in table {tablename} 
        """,
        "query": """copy into @unload_snowflake_{DATE_TIME} from 
                  (     
                        your select query	
                  ) 
                  OVERWRITE = True file_format = unload_snowflake_Report_{DATE_TIME} HEADER = True max_file_size=4900000000;
        """,
        "drop_file": "DROP FILE FORMAT IF EXISTS unload_snowflake_Report_{DATE_TIME}",
        "drop_stage":"Drop stage IF EXISTS unload_snowflake_{DATE_TIME}"     
    }
}