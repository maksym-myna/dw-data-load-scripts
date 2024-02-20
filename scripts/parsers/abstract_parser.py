from abc import ABC, abstractmethod
import orjson
import csv
import os


class AbstractParser(ABC):
    """Abstract base class for parsers."""

    def __init__(self, file_type: str):
        self._write_strategy = self._write_jsonl if file_type == 'jsonl' else self._write_csv
        self.type_name = file_type

    @abstractmethod
    def process_file(self, input_file: str, output: str) -> str:
        """
        Abstract method to process a file.

        Args:
            input_file: The input file path.
            output: The output file path.
        """
        

    @classmethod
    def _write_jsonl(cls, file: str, obj)-> None:
        file.write(orjson.dumps(obj).decode('utf-8') + '\n')

    @classmethod
    def _write_csv(cls, file: str, obj) -> None:
        writer = csv.DictWriter(file, fieldnames=obj.keys())
        writer.writerow(obj)

    @staticmethod
    def is_path_valid(path: str) -> bool:
        """
        Check if the given path is valid.

        Args:
            path (str): The path to be checked.

        Returns:
            bool: True if the path is valid, False otherwise.
        """
        return os.path.abspath('.') in os.path.abspath(path)
