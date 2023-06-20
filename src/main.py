from config import *
from snowflake_connection import *
from snowflake_unloader import *

if __name__ == '__main__':

    try:
        LOGGER.info("***************Starting Snowflake Connecting*******************")
        
        loader = Snowflake_Unloader()
        loader.SnowflakeAccess()
        loader.unload()  
        
        LOGGER.info("------------Snowflake Connected Successfully----------------")
        
    except Exception as e:
        LOGGER.error(str(e), exc_info=True)
        LOGGER.info("Error Executing Snowflake Unloading")
