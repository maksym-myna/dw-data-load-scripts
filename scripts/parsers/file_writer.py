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
        if file_type == "jsonl":
            self._write_strategy = self.__write_jsonl
            self._tuple_write_strategy = self.__write_jsonl_sqlite_tuple
        elif file_type == "csv":
            self._write_strategy = self.__write_csv
            self._tuple_write_strategy = self.__write_csv_tuple

        self.type_name = file_type

    @staticmethod
    def __write_csv(file: TextIOWrapper, obj: dict) -> None:
        """
        Writes a dictionary object to a CSV file.

        Args:
            file (str): The file path to write to.
            obj (dict): The dictionary object to write.

        Returns:
            None
        """
        writer = csv.DictWriter(file, fieldnames=obj.keys(), quoting=csv.QUOTE_ALL)
        writer.writerow(obj)

    @staticmethod
    def __write_csv_tuple(file: TextIOWrapper, rows: list[any]) -> None:
        """
        Writes a list of dictionary objects to a CSV file.

        Args:
            file (str): The file path to write to.
            rows (list[any]): Rows fetched from database.

        Returns:
            None
        """
        writer = csv.writer(file, quoting=csv.QUOTE_ALL)
        writer.writerows(rows)

    @staticmethod
    def __write_jsonl(file: TextIOWrapper, obj: dict) -> None:
        """
        Writes a dictionary object to a JSONL file.

        Args:
            file (TextIOWrapper): The file object to write to.
            obj (dict): The dictionary object to write.

        Returns:
            None
        """
        file.write(orjson.dumps(obj).decode("utf-8") + "\n")

    @staticmethod
    def __write_jsonl_sqlite_tuple(file: TextIOWrapper, rows: list[any]) -> None:
        """
        Writes a list of dictionary objects to a JSONL file.

        Args:
            file (TextIOWrapper): The file path to write to.
            rows (list[any]): Rows fetched from database.

        Returns:
            None
        """
        for row in rows:
            orjson.dumps(row, file)
            file.write("\n")
