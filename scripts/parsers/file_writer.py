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
        if file_type == 'jsonl':
            self._write_strategy = self.__write_jsonl
            self._list_write_strategy = self.__write_jsonl_list
            self._dict_write_strategy = self.__write_jsonl_dict
            self._sqlite_write_strategy = self.__write_jsonl_sqlite
        else:
            self._write_strategy = self.__write_csv
            self._list_write_strategy = self.__write_csv_list
            self._dict_write_strategy = self.__write_csv_dict
            self._sqlite_write_strategy = self.__write_csv_sqlite

        self.type_name = file_type

    @classmethod
    def __write_csv(cls, file: TextIOWrapper, obj: dict) -> None:
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

    @classmethod
    def __write_csv_list(cls, file: TextIOWrapper, obj_list: list[dict]) -> None:
        """
        Writes a list of dictionary objects to a CSV file.

        Args:
            file (str): The file path to write to.
            obj_list (List[dict]): The list of dictionary objects to write.

        Returns:
            None
        """
        writer = csv.DictWriter(file, fieldnames=obj_list[0].keys(), quoting=csv.QUOTE_ALL)
        writer.writerows(obj_list)
    
    @classmethod
    def __write_csv_dict(cls, file: TextIOWrapper, dict: dict) -> None:
        """
        Writes a dictionary object to a CSV file.

        Args:
            file (str): The file path to write to.
            dict (dict): Dictionary object to write.

        Returns:
            None
        """
        writer = csv.writer(file, quoting=csv.QUOTE_ALL)
        for key, value in dict.items():
            writer.writerow([value, key])

    @classmethod
    def __write_csv_sqlite(cls, file: TextIOWrapper, rows:list[any]) -> None:
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

    @classmethod
    def __write_jsonl(cls, file: TextIOWrapper, obj: dict) -> None:
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
    def __write_jsonl_list(cls, file: TextIOWrapper, obj_list: list[dict]) -> None:
        """
        Writes a list of dictionary objects to a JSONL file.

        Args:
            file (TextIOWrapper): The file object to write to.
            obj_list (List[dict]): The list of dictionary objects to write.

        Returns:
            None
        """
        file.write('\n'.join(orjson.dumps(obj).decode('utf-8') for obj in obj_list) + '\n')

    @classmethod
    def __write_jsonl_dict(cls, file: TextIOWrapper, dict: dict) -> None:
        """
        Writes a dictionary object to a JSONL file.

        Args:
            file (TextIOWrapper): The file path to write to.
            dict (dict): Dictionary object to write.

        Returns:
            None
        """
        for key, value in dict.items():
            orjson.dumps({key: value}, file)
            file.write('\n')

    @classmethod
    def __write_jsonl_sqlite(cls, file: TextIOWrapper, rows: list[any]) -> None:
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
            file.write('\n')
