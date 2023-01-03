from sql.queries import queries
from services.connection import make_conn
from services.services import Reader, Writer
import click as cl


@cl.command
@cl.option(
    "-r", "--rooms-path", help="Path to file with data about rooms",
    type=cl.STRING
)
@cl.option(
    "-s",
    "--stud-path",
    help="Path to file with data about students",
    type=cl.STRING,
)
@cl.option("-o", "--out-path", type=cl.STRING, help="Output file path")
@cl.option(
    "-f",
    "--out-format",
    help="Output file format",
    type=cl.Choice(["json", "xml"]),
)
def main(rooms_path: str, stud_path: str,
         out_format: str, out_path: str):
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
