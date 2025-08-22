import pytest, json
import logging
from src.etl.etl import CSVLoader, JSONLoader, TextLoader
import tempfile
from pathlib import Path

logger = logging.getLogger("test_logger")
logger.setLevel(logging.WARNING)


class TestCSVLoader:

    @pytest.fixture
    def temp_csv_file(self):
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as f:
            f.write(
                "Client,Date,Channel,Campaign_id,Spend_usd\nDummy,2024-06-21,Google,camp_007,754.47"
            )
            f_path = Path(f.name)
        yield str(f_path)
        f_path.unlink()

    def test_valid_filename(self, temp_csv_file):
        fullpath = temp_csv_file
        loader = CSVLoader(fullpath)
        assert loader._validate_file() is None

    def test_invalid_filename_extension(self, tmp_path):
        bad_file = tmp_path / "badfile.txt"
        bad_file.write_text("some data,to,test\n")

        loader = CSVLoader(str(bad_file))

        with pytest.raises(ValueError, match="Invalid file extension. Check file."):
            loader._validate_file()

    def test_invalid_filename_pattern(self, tmp_path, caplog):
        bad_file = tmp_path / "AD_SPEND_DUMMY_2025.csv"
        bad_file.write_text(
            "Client,Date,Channel,Campaign_id,Spend_usd\nDummy,2024-06-21,Google,camp_007,754.47"
        )

        loader = CSVLoader(str(bad_file))

        with caplog.at_level(logging.WARNING, logger="etl.etl"):
            loader._validate_file()

        assert any(
            "Invalid filename pattern. Check file." in msg for msg in caplog.messages
        )

    def test_valid_columns(self, tmp_path, caplog):
        bad_columns = tmp_path / "AD_SPEND_DUMMY_20250819.csv"
        bad_columns.write_text(
            "Client,Date,,Campaign_id,Spend_usd\nDummy,2024-06-21,Google,camp_007,754.47"
        )

        loader = CSVLoader(str(bad_columns))

        with caplog.at_level(logging.WARNING, logger="etl.etl"):
            loader._validate_file()

        assert any(
            f"missing the required columns. Skipping file." in msg
            for msg in caplog.messages
        )
        assert any(loader.filename in msg for msg in caplog.messages)


class TestJSONLoader:

    def test_valid_filename(self, tmp_path):
        data = {
            "client": "Dummy",
            "date": "20250819",
            "channel": "Dummy",
            "impressions": 99,
            "clicks": 99,
            "conversions": 99,
            "cost_per_click": 1.00,
        }

        temp_file = tmp_path / "CLICKSTREAMS_DUMMY_20250819.json"
        with temp_file.open("w") as f:
            json.dump(data, f)

        loader = JSONLoader(str(temp_file))
        assert loader._validate_file() is None

    def test_invalid_filename_extension(self, tmp_path):
        bad_file = tmp_path / "badfile.txt"
        bad_file.write_text("some data,to,test\n")

        loader = JSONLoader(str(bad_file))
        loader._validate_file()

    def test_invalid_filename_pattern(self, tmp_path, caplog):
        bad_file = tmp_path / "CLICKSTREAMS_DUMMY_2025.json"
        bad_file.write_text("some data,to,test\n")

        loader = JSONLoader(str(bad_file))

        with caplog.at_level(logging.WARNING, logger="etl.etl"):
            loader._validate_file()

        assert any(
            "Invalid filename pattern. Check file" in msg for msg in caplog.messages
        )

    def test_invalid_parse_json(self, tmp_path, caplog):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("invalid: json")

        loader = JSONLoader(str(bad_file))

        with caplog.at_level(logging.ERROR, logger="etl.etl"):
            raw_json = bad_file.read_text()
            result = loader._parse_records(raw_json)

        assert result is None
        assert any("Failed to parse JSON" in msg for msg in caplog.messages)


class TestTextLoader:

    @pytest.fixture
    def temp_txt_file(self):
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt", delete=False) as f:
            f.write("client: Dummy | date: 08/19/2025 | channel: Dummy | event: dummy")
            f_path = Path(f.name)
        yield str(f_path)
        f_path.unlink()

    def test_valid_filename(self, temp_txt_file):
        fullpath = temp_txt_file
        loader = TextLoader(fullpath)
        assert loader._validate_file() is None

    def test_invalid_filename_extension(self, tmp_path):
        bad_file = tmp_path / "badfile.json"
        bad_file.write_text("some data,to,test\n")

        loader = TextLoader(str(bad_file))
        loader._validate_file()

    def test_invalid_filename_pattern(self, tmp_path, caplog):
        bad_file = tmp_path / "PERFORMANCE_DUMMY_2025.txt"
        bad_file.write_text("some data,to,test\n")

        loader = TextLoader(str(bad_file))

        with caplog.at_level(logging.WARNING, logger="etl.etl"):
            loader._validate_file()

        assert any(
            "Invalid filename pattern. Check file" in msg for msg in caplog.messages
        )

    def test_invalid_parse_txt(self, tmp_path, caplog):
        bad_file = tmp_path / "bad.txt"
        bad_file.write_text("some data,to,test\n")

        loader = TextLoader(str(bad_file))

        with caplog.at_level(logging.ERROR, logger="etl.etl"):
            raw_txt = bad_file.read_text()
            result = loader._parse_records(raw_txt)

        assert result is None
        assert any("Failed to parse text" in msg for msg in caplog.messages)
