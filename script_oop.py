import json
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s",
                    encoding='utf-8')


class Manager_db():
    def __init__(self, format='json'):
        self.format = format

    def load_json(self, file: str) -> list:
        '''load and convert json'''
        with open(file, 'r') as f:
            return json.load(f)

    def connection(self, dbname: str, user: str,
                   password: str, host: str,
                   port: str, autocommit: bool = True) -> None:
        '''Establishing the connection and creating a cursor object'''
        try:
            conn = psycopg2.connect(
                database=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )

            # Setting auto commit
            conn.autocommit = autocommit

            # Creating a cursor object using the cursor() method
            global cursor
            cursor = conn.cursor()
            logging.info('Connected succesfully')
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Failed to connect. {error}", exc_info=True)

    def insertion(self, data: list, table: str, columns: list) -> None:
        '''Insertion data to database.
           Insertion is available after connection to database'''
        try:
            counter = 0
            for row in data:
                cursor.execute(f"INSERT INTO {table} VALUES (" +
                               ('\'{}\', ' * len(columns))[:-2]
                               .format(*[row[col] for col in columns]) + ")")
                counter += 1
            logging.info(f'Sucessfully inserted {counter} rows')
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Failed to insert records into {table} table. \
                          '{error}'", exc_info=True)

    def get_info(self, query: str) -> list:
        '''Getting information from database and converting it to JSON file'''
        cursor.execute(query)
        return cursor.fetchall()

    def save_file(self, info_from_db: list,
                  filename: str, indent: int = 4) -> None:
        '''Saving file in necessary format'''
        with open(f"{filename}.{self.format}", "w") as file:
            file.write(json.dumps(info_from_db, indent=indent))
        logging.info(f'File {filename}.{self.format} succesfully saved')


class Student(Manager_db):
    def __init__(self, students):
        self.students = students

    def load_json(self) -> list:
        '''load and convert json'''
        with open(self.students, 'r') as f:
            return json.load(f)

    def insertion(self, data: list, table: str = 'students',
                  columns: list = ['id', 'name', 'room',
                                   'sex', 'birthday']) -> None:
        '''Insertion data to database.
           Insertion is available after connection to database'''
        try:
            counter = 0
            for row in data:
                cursor.execute(f"INSERT INTO {table} VALUES (" +
                               ('\'{}\', ' * len(columns))[:-2]
                               .format(*[row[col] for col in columns]) + ")")
                counter += 1
            logging.info(f'Sucessfully inserted {counter} rows')
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Failed to insert records into {table} table. \
                          '{error}'", exc_info=True)


class Room(Manager_db):
    def __init__(self, rooms):
        self.rooms = rooms

    def load_json(self) -> list:
        '''load and convert json'''
        with open(self.rooms, 'r') as f:
            return json.load(f)

    def insertion(self, data: list, table: str = 'rooms',
                  columns: list = ['id', 'name']) -> None:
        '''Insertion data to database.
           Insertion is available after connection to database'''
        try:
            counter = 0
            for row in data:
                cursor.execute(f"INSERT INTO {table} VALUES (" +
                               ('\'{}\', ' * len(columns))[:-2]
                               .format(*[row[col] for col in columns]) + ")")
                counter += 1
            logging.info(f'Sucessfully inserted {counter} rows')
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Failed to insert records into {table} table. \
                          '{error}'", exc_info=True)


manager = Manager_db()

# Make a connection to db
manager.connection(dbname="python_intro",
                   user='postgres',
                   password='1',
                   host='localhost',
                   port='5432')

# Getting json
rooms = Room('rooms.json')
room_data = rooms.load_json()

students = Student('students.json')
stud_data = students.load_json()

# Insertion data to db
rooms.insertion(room_data)
students.insertion(stud_data)

# list of rooms and count of students in that rooms. 1st query
query1 = manager.get_info('''
                          SELECT
                              r.room_id,
                              r.room_name,
                              COUNT(s.student_id) AS count_of_students
                          FROM rooms r
                              LEFT JOIN students s USING(room_id)
                          GROUP BY 1
                          ORDER BY 1
                          ''')
manager.save_file(query1, 'query1')

# top 5 rooms with the smallest avg age of students in the rooms. 2nd query
query2 = manager.get_info('''
                          SELECT
                              *
                          FROM (
                              SELECT
                                  *,
                                  ROW_NUMBER() OVER(ORDER BY avg_stud_age)
                                  AS placement
                              FROM (
                                  SELECT
                                   r.room_id,
                                   r.room_name,
                                   ROUND(AVG(EXTRACT(YEAR FROM s.birthday)), 2)
                                   :: FLOAT AS avg_stud_age
                                  FROM rooms AS r
                                   JOIN students AS s USING(room_id)
                                GROUP BY 1
                                ) AS tmp
                              ) AS tmp2
                          WHERE placement < 6
                          ''')
manager.save_file(query2, 'query2')

# top 5 rooms with the highest difference
# in age of students in the rooms. 3rd query
query3 = manager.get_info('''
                          SELECT
                            *
                          FROM (
                              SELECT
                                  *,
                                  ROW_NUMBER() OVER(ORDER BY age_diff DESC)
                                  AS placement
                              FROM (
                                      SELECT
                                          r.room_id,
                                          r.room_name,
                                          MAX(EXTRACT(YEAR FROM s.birthday)) -
                                          MIN(EXTRACT(YEAR FROM s.birthday))
                                          :: FLOAT AS age_diff
                                      FROM rooms AS r
                                          JOIN students AS s USING(room_id)
                                      GROUP BY 1
                                      ) AS tmp
                              ) AS tmp2
                          WHERE placement < 6
                          ''')
manager.save_file(query3, 'query3')

# list of rooms with different sex in one room. 4th query
query4 = manager.get_info('''
                          SELECT room_name
                          FROM (
                                SELECT
                                    r.room_id,
                                    r.room_name,
                                    COUNT(DISTINCT s.sex) AS gender_variety
                                FROM rooms AS r
                                    JOIN students AS s USING(room_id)
                                GROUP BY 1
                                HAVING COUNT(DISTINCT s.sex) > 1
                                ) AS tmp
                          ''')
manager.save_file(query4, 'query4')
