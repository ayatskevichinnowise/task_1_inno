import psycopg as pg
from psycopg.rows import dict_row
import logging
import logging.config

logging.config.fileConfig(fname="config/logging.conf")


def make_conn(dbname: str, user: str,
              password: str, host: str,
              port: str) -> pg.Connection:
    '''
    Function that makes a connetction to database
    :param dbname: database name
    :param user: user name
    :param password: password for connection
    :param host: host
    :param port: port
    :return: connection to database
    '''
    try:
        connection = pg.connect(dbname=dbname,
                                user=user,
                                password=password,
                                host=host,
                                port=port,
                                autocommit=True,
                                row_factory=dict_row
                                )
        logging.info('Connected to database succesfully')
    except (Exception, pg.Error) as error:
        logging.error(f"Failed to connect. {error}", exc_info=True)
    return connection
