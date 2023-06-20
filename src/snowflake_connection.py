import snowflake.connector
from log_generator import *


class Snowflake_Connector():

    def connectSnowflake(self, account,user,password,warehouse,database,schema,role):
        try:
            self.engine = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
                authenticator = 'externalbrowser'
            )

            self.is_valid_connection = self.get_version()
            LOGGER.debug('snowflake connection ' + 'successful!' if self.is_valid_connection else 'Failed!')
            
            if not self.is_valid_connection:
                raise Exception('Snowflake connection failed!')

        except Exception as e:
            LOGGER.error("Snowflake Connection error - " + str(e))

    def get_version(self):
        cs = self.engine.cursor()
        try:
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            LOGGER.info('Snowflake version-' + str(one_row[0]))
            return True
        except Exception as e:
            LOGGER.error(
                'Snowflake connection test failed. Exception-' + str(e).rstrip())
            return False
        finally:
            cs.close()

    def is_conn_live(self):
        try:
            if self.engine and not self.engine.is_closed():
                return self.get_version()
            else:
                return False
        except Exception as e:
            return False

    def run_fetch_one(self, query):
        if not self.is_conn_live():
            return None
        cur = self.engine.cursor()
        try:
            print(query)
            result = cur.execute(query)
            row = result.fetchone()

            return row
        except Exception as e:
            msg = "Error while executing query. Query-%s;Exception-%s" % (
                str(query), str(e).rstrip())
            LOGGER.error(msg)
            raise e
        finally:
            try:
                cur.close()
            except Exception as e:
                LOGGER.warn(
                    "Error while closing snowflake connection. Exception-"+str(e).rstrip())     

    def run_fetch_all(self, query):
        if not self.is_conn_live():
            return None
        cur = self.engine.cursor()
        try:
            result = cur.execute(query)
            row = result.fetchall()

            return row
        except Exception as e:
            msg = "Error while executing query. Query-%s;Exception-%s" % (
                str(query), str(e).rstrip())
            LOGGER.error(msg)
            raise e
        finally:
            try:
                cur.close()
            except Exception as e:
                LOGGER.warn(
                    "Error while closing snowflake connection. Exception-"+str(e).rstrip())                         
    
    def get_all_columns_from_table(self,  tablename):
            return self.run_fetch_all( f'show columns in table {tablename}')   