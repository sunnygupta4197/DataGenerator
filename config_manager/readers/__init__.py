import os
from pathlib import Path
from typing import Union

from .json_reader import JSONConfigReader
from .excel_reader import FileToJSONConverter


class ConfigReader:
    def __init__(self, config_path: Union[str, Path] =None):
        self.config_path = config_path

    def read_json_config(self) -> dict:
        config_reader = JSONConfigReader(self.config_path)
        return config_reader.load_config()

    def read_excel_config(self) -> dict:
        excel_converter = FileToJSONConverter()
        return excel_converter.convert(self.config_path)

    def get_file_extension(self) -> str:
        base_file_name, file_ext = os.path.splitext(self.config_path)
        return file_ext

    def read_config(self) -> dict:
        file_ext = self.get_file_extension()
        if file_ext in ['.excel', '.csv', '.xlsx', '.xls']:
            return self.read_excel_config()
        return self.read_json_config()