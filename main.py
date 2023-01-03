from sql.queries import queries
from services.connection import make_conn
from services.services import Reader, Writer
import click


@click.command
@click.option(
    "--rooms_path",
    help="Path to the file with data about rooms",
    type=click.STRING,
    default='data\\rooms.json',
    show_default=True
)
@click.option(
    "--stud_path",
    help="Path to the file with data about students",
    type=click.STRING,
    default='data\\students.json',
    show_default=True
)
@click.option(
    "--out_path",
    help="Output file path",
    type=click.STRING,
    default='',
    show_default=True
    )
@click.option(
    "--out_format",
    help="Output file format",
    type=click.Choice(['json', 'xml']),
    default='json',
    show_default=True
    )
def main(rooms_path: str, stud_path: str,
         out_format: str, out_path: str) -> None:
    '''
    The main function that gets all dependencies and runs script.
    :param rooms_path: path to the file with data about rooms
    :param stud_path: path to the file with data about students
    :param out_format: save file format (can only be JSON or XML)
    :param out_path: save file path
    :return: None
    '''
    connection = make_conn()

    rooms = Writer(connection, 'rooms')
    rooms.write(rooms.get(rooms_path))

    stud = Writer(connection, 'students')
    stud.write(rooms.get(stud_path))

    reader = Reader(connection)
    for name, query in queries.items():
        reader.save_file(query, name, out_format, out_path)


if __name__ == "__main__":
    main()
