import logging.handlers
from datetime import datetime
import os

dir_path = os.path.dirname(os.path.abspath(__file__))
today = datetime.now()
str_today = today.strftime('%Y-%m-%d')


log_file_dir =  'Log'
if not os.path.isdir(log_file_dir):
    os.makedirs(log_file_dir)


# Log File Settings
LOG_FORMAT = ('%(asctime)s|%(levelname)s|%(funcName)s| %(message)s')
LOG_FILENAME = 'Log/' + str_today + '_' + 'snowflake_unloading.log'

# Logging Format
LOGGER = logging.getLogger( __name__ )
LOGGER.setLevel( logging.DEBUG )
handler = logging.handlers.RotatingFileHandler( LOG_FILENAME, maxBytes=2000000, backupCount=5, mode = 'w')
formatter = logging.Formatter( LOG_FORMAT )
handler.setFormatter( formatter )
LOGGER.addHandler( handler )
LOGGER.addHandler( logging.StreamHandler() )

LOG_LEVEL = logging.INFO
MAX_LOG_BYTES = 2000000
MAX_OLD_LOGS = 50