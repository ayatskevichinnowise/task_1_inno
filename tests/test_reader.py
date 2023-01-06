import json
import pytest as pt
from services.services import Reader


class TestReader:
    @pt.fixture
    def data(self):
        return [{'id': 0, 'name': 'Room #0'},
                {'id': 1, 'name': 'Room #1'},
                {'id': 2, 'name': 'Room #2'},
                {'id': 3, 'name': 'Room #3'},
                {'id': 4, 'name': 'Room #4'}]

    @pt.fixture
    def reader(self, mocker):
        connection = mocker.stub(name="connection")
        return Reader(connection)

    @pt.fixture
    def filename(self):
        return "test_file"

    @pt.fixture
    def format(self):
        return "json"

    @pt.fixture
    def query(self, mocker):
        return mocker.stub(name="query")

    def test_get_info(self, reader, mocker, data, query):
        expected = [{'id': 0, 'name': 'Room #0'},
                    {'id': 1, 'name': 'Room #1'},
                    {'id': 2, 'name': 'Room #2'},
                    {'id': 3, 'name': 'Room #3'},
                    {'id': 4, 'name': 'Room #4'}]
        mocker.patch.object(reader, "connection")
        reader.connection.execute(query).fetchall.return_value = data
        actual = reader.get_info(query)
        assert actual == expected

    def test_save_file(self, reader, query, mocker,
                       filename, format, data, tmp_path):
        path = tmp_path / f'{filename}.{format}'
        path.touch()
        mocker.patch.object(reader, "connection")
        reader.connection.execute(query).fetchall.return_value = data
        reader.save_file(query, filename, format, out_path=tmp_path)
        actual = json.loads(path.read_text())
        assert actual == data
