import os
from pathlib import Path

from .json_reader import JSONConfigReader
from .csv_reader import CSVToJSONConverter
from .excel_reader import FileToJSONConverter


class ConfigReader:
    def __init__(self, config_path: str | Path =None):
        self.config_path = config_path

    def read_json_config(self) -> dict:
        config_reader = JSONConfigReader(self.config_path)
        return config_reader.load_config()

    def read_csv_config(self) -> dict:
        csv_converter = CSVToJSONConverter(self.config_path)
        return csv_converter.convert(self.config_path)

    def read_excel_config(self) -> dict:
        excel_converter = FileToJSONConverter()
        return excel_converter.convert(self.config_path)

    def get_file_extension(self) -> str:
        base_file_name, file_ext = os.path.splitext(self.config_path)
        return file_ext

    def read_config(self) -> dict | None:
        file_ext = self.get_file_extension()
        if file_ext == '.json':
            return self.read_json_config()
        elif file_ext == '.csv':
            return self.read_csv_config()
        elif file_ext in ['.excel', '.xlsx', '.xls']:
            return self.read_excel_config()
        return None