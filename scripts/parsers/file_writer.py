from io import TextIOWrapper
import orjson
import csv 

class FileWriter:
    """
    A class that provides functionality to write data to different file types.

    Args:
        file_type (str): The type of file to write ('jsonl' or 'csv').

    Attributes:
        _write_strategy (function): The write strategy based on the file type.
        type_name (str): The type of file being written.

    Methods:
        _write_jsonl: Writes data to a JSONL file.
        _write_csv: Writes data to a CSV file.
    """

    def __init__(self, file_type):
        self._write_strategy = self._write_jsonl if file_type == 'jsonl' else self._write_csv
        self.type_name = file_type

    @classmethod
    def _write_jsonl(cls, file: TextIOWrapper, obj: dict) -> None:
        """
        Writes a dictionary object to a JSONL file.

        Args:
            file (TextIOWrapper): The file object to write to.
            obj (dict): The dictionary object to write.

        Returns:
            None
        """
        file.write(orjson.dumps(obj).decode('utf-8') + '\n')

    @classmethod
    def _write_csv(cls, file: str, obj: dict) -> None:
        """
        Writes a dictionary object to a CSV file.

        Args:
            file (str): The file path to write to.
            obj (dict): The dictionary object to write.

        Returns:
            None
        """
        try:
            writer = csv.DictWriter(file, fieldnames=obj.keys(), quoting=csv.QUOTE_ALL)
            writer.writerow(obj)
        except Exception:
            pass