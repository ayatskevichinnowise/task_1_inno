import json
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s", encoding='utf-8')

def load_json(file: str) -> list:
    '''load and convert json'''
    with open(file, 'r') as f:
        return json.load(f)

def connection(dbname: str, user: str, 
               password: str, host: str, 
               port: str, autocommit: bool) -> None:
    '''Establishing the connection and creating a cursor object'''
    try:
        conn = psycopg2.connect(
            database=dbname, 
            user=user, 
            password=password, 
            host=host, 
            port= port
        )

        #Setting auto commit
        conn.autocommit = autocommit

        #Creating a cursor object using the cursor() method
        global cursor
        cursor = conn.cursor()
        logging.info('Connected succesfully')
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Failed to connect. {error}")

def insertion(data: list, table: str, columns: list) -> None:
    '''Insertion data to database. Insertion is available after connection to database'''
    try:
        counter = 0
        for row in data:
            cursor.execute(f"INSERT INTO {table} VALUES (" + ('\'{}\', ' * len(columns))[:-2].format(*[row[col] for col in columns]) + ")")
            counter += 1
        logging.info(f'Sucessfully inserted {counter} rows')
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Failed to insert records into {table} table. '{error}'", exc_info=True)

def get_info(query: str, filename: str, indent: int=4) -> None:
    '''Getting information from database and converting it to JSON file'''
    cursor.execute(query)

    info_from_db = cursor.fetchall()

    # writing to json
    with open(f"{filename}.json", "w") as file:
        file.write(json.dumps(info_from_db, indent=indent))
    logging.info(f'File {filename}.json succesfully saved')

def main():
    room_data = load_json('rooms.json')
    stud_data = load_json('students.json')

    connection(dbname="python_intro", 
            user='postgres', 
            password='1', 
            host='localhost', 
            port= '5432',
            autocommit=True)

    insertion(room_data, 'rooms', columns=['id', 'name'])
    insertion(stud_data, 'students', columns=['id', 'name', 'room', 'sex', 'birthday'])

    # list of rooms and count of students in that rooms. 1st query
    get_info(query = '''
                    SELECT 
                        r.room_id,
                        r.room_name,
                        COUNT(s.student_id) AS count_of_students
                    FROM rooms r 
                        LEFT JOIN students s USING(room_id)
                    GROUP BY 1 
                    ORDER BY 1
                    ''',
            filename='query1')

    # top 5 rooms with the smallest avg age of students in the rooms. 2nd query
    get_info(query = '''
                    SELECT 
                        *
                    FROM (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER(ORDER BY avg_stud_age) AS placement
                        FROM (
                        
                            SELECT 
                                r.room_id,
                                r.room_name,
                                ROUND(AVG(EXTRACT(YEAR FROM s.birthday)), 2) :: FLOAT AS avg_stud_age
                            FROM rooms AS r
                                JOIN students AS s USING(room_id) 
                            GROUP BY 1
                    ) AS tmp
                        ) AS tmp2
                    WHERE placement < 6
                    ''',
            filename='query2')

    # top 5 rooms with the highest difference in age of students in the rooms. 3rd query
    get_info(query = '''
                    SELECT 
                        *
                    FROM (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER(ORDER BY age_diff DESC) AS placement
                        FROM (
                        
                            SELECT 
                                r.room_id,
                                r.room_name,
                                MAX(EXTRACT(YEAR FROM s.birthday)) - MIN(EXTRACT(YEAR FROM s.birthday)) :: FLOAT AS age_diff
                            FROM rooms AS r
                                JOIN students AS s USING(room_id) 
                            GROUP BY 1
                            ) AS tmp
                        ) AS tmp2
                    WHERE placement < 6
                    ''',
            filename='query3')

    # list of rooms with different sex in one room. 4th query
    get_info(query = '''
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
                    ''',
            filename='query4')

if __name__ == "__main__":
    main()