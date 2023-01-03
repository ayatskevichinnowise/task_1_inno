import psycopg2 as pg
from credentials import credentials
import logging


def make_conn():
    logging.basicConfig(level=logging.INFO,
                        filename="py_log.log",
                        filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s",
                        encoding='utf-8')
    try:
        connection = pg.connect(dbname=credentials.get('DBNAME'),
                                user=credentials.get('USER'),
                                password=credentials.get('PASSWORD'),
                                host=credentials.get('HOST'),
                                port=credentials.get('PORT')
                                )
        logging.info('Connected to database succesfully')
    except (Exception, pg.Error) as error:
        logging.error(f"Failed to connect. {error}", exc_info=True)
    return connection
