import pytest as pt
from services.services import Writer


class TestWriter:
    @pt.fixture
    def table(self):
        return ""

    @pt.fixture
    def writer(self, mocker, table):
        connection = mocker.stub(name="connection")
        return Writer(connection, table)

    @pt.fixture
    def data(self):
        return ""

    @pt.fixture
    def initdir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        path = tmp_path.joinpath("test_file.txt")
        path.write_text("# testdata")
        return path

    def test_write(self, writer, mocker, data):
        mocker.patch.object(writer, "connection")
        writer.connection.execute.return_value = None
        writer.write(data)
        writer.connection.execute.assert_called_once()

    def test_get(self, writer, initdir):
        actual = writer.get(str(initdir))
        assert 'testdata' in actual
