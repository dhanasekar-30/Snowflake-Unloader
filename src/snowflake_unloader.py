from config import *
from log_generator import *
from snowflake_connection import *
from random import randint


class Snowflake_Unloader():

    def __init__(self) -> None:
        try:
            num = randint(100,999)
            self.FEED_DATETIME = today.strftime("%Y%m%d_%H%M%S") +'_' + str(num)

            self.snow = Snowflake_Connector()
            self.reload_count = 0

        except Exception as e:
            LOGGER.error("Initialize error - " + str(e))    
            raise e
        
    def SnowflakeAccess(self):
        try:
            self.snow.connectSnowflake(ACCOUNT,USERNAME,PASSWORD,WAREHOUSE,DATABASE,SCHEMA,ROLE)
        except Exception as e:
            LOGGER.error("Snowflake Connection - " + str(e))    
            raise e    
    
    def unload(self):
        LOGGER.info("=================Started Snowflake Unload=======================")
        self.FEED_DATETIME = today.strftime("%Y%m%d_%H%M%S")
        try:
            self.FEED_DATE = today
            self.FEED_DATE = self.FEED_DATE.strftime('%Y-%m-%d')
            for Table in Table_Names:
                self.FEED_INFO = FEED_EXPORT_CONFIG['TCB_PA']
                self.fetch_FeedTemplates()                
                self.replace_keys(Table)
                self.run_commands()
                print("Snowflake Unload completed for " + Table) 
            print("Snowflake Unload completed for all the Tables ")                
            return True
        
        except Exception as e:
            LOGGER.error('Snowflake Loading error - ' + str(e))
            raise e

    def fetch_FeedTemplates(self):
        self.file_format = self.FEED_INFO['file_format']
        self.snow_stage = self.FEED_INFO['stage_template']
        self.query = self.FEED_INFO['query']
        self.drop_file = self.FEED_INFO['drop_file']
        self.drop_stage = self.FEED_INFO['drop_stage']
        self.table = self.FEED_INFO['table_name']

    def replace_keys(self, Table):
        try:
            self.file_format = self.file_format.replace('{DATE_TIME}',self.FEED_DATETIME)
            self.snow_stage = self.snow_stage.replace('{DATE}','run=' + self.FEED_DATE)
            self.snow_stage = self.snow_stage.replace('{ACCESS_KEY}',AccessKey)
            self.snow_stage = self.snow_stage.replace('{SECRET_KEY}',SecretKey)
            self.snow_stage = self.snow_stage.replace('{DATE_TIME}',self.FEED_DATETIME)
            self.snow_stage = self.snow_stage.replace('{TABLE}',Table)
            self.drop_file = self.drop_file.replace('{DATE_TIME}',self.FEED_DATETIME)
            self.drop_stage = self.drop_stage.replace('{DATE_TIME}',self.FEED_DATETIME)
            self.query = self.query.replace('{DATE_TIME}',self.FEED_DATETIME)
            self.query = self.query.replace('{tablename}', Table)
            print(self.query)
            
        except Exception as e:
            LOGGER.error('Snowflake Loading error - ' + str(e))
            raise e
        
    def run_commands(self):
        try:                 
            msg = self.snow.run_fetch_one(self.file_format)
            LOGGER.info(msg)
            msg = self.snow.run_fetch_one(self.snow_stage)
            LOGGER.info(msg)
            self.load_msg = self.snow.run_fetch_all(self.query)
            LOGGER.info(self.load_msg)
            print(self.load_msg)
            msg = self.snow.run_fetch_one(self.drop_file)
            LOGGER.info(msg)
            msg = self.snow.run_fetch_one(self.drop_stage)
            LOGGER.info(msg)   
        except Exception as e:
            LOGGER.error('Snowflake Unloading error - ' + str(e))  
            raise e  
