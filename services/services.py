import os
import json
import logging
import psycopg as pg
from psycopg import sql
from sql.queries import insertion
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString

logging.basicConfig(level=logging.INFO,
                    filename="py_log.log",
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s",
                    encoding='utf-8')


class Writer:
    '''
    Class that writes information to Postgres database
    '''
    def __init__(self, connection: pg.Connection,
                 table: str):
        self.connection = connection
        self.table = table

    def get(self, path: str) -> str:
        '''
        Method that gets information from JSON file
        :path: path to file with necessary data
        :return: data in string format
        '''
        with open(path, "r") as file:
            return file.read()

    def write(self, data: str) -> None:
        '''
        Method that writes information to database
        from JSON files
        :data: necessary data for uploading to database
        :return: None
        '''
        self.connection.execute(
                    sql.SQL(insertion["json_array"]).format(
                     table=sql.Identifier(self.table),
                     json=sql.Literal(data)
                    )
                )
        logging.info(f"Wrote data to {self.table} table")


class Reader:
    '''
    Class that gets information from Postrgres database
    '''
    def __init__(self, connection: pg.Connection):
        self.connection = connection

    def get_info(self, query: str) -> list:
        '''
        Method that gets information from database
        :param query: query for execution
        :return: result of query as list
        '''
        return self.connection.execute(query).fetchall()

    def save_file(self, query: str, filename: str,
                  format: str = 'json', out_path: str = '',
                  indent: int = 4) -> None:
        '''
        Method that saves file with information in JSON or XML formats
        :query: query for execution
        :filename: save file name
        :format: save file format (can only be JSON or XML)
        :out_path: save file path
        :indent: indentation in the file
        :result: None
        '''
        full_path = os.path.join(out_path, f'{filename}.{format}')
        with open(full_path, 'w') as file:
            if format == 'json':
                file.write(json.dumps(self.get_info(query), indent=indent))
            elif format == 'xml':
                xml = dicttoxml([dict(row) for row in self.get_info(query)],
                                attr_type=False)
                file.write(parseString(xml).toprettyxml())
            else:
                logging.error('Wrong format for output file')
        logging.info(f'Succesfully save a {full_path}')
