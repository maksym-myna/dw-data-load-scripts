from abc import ABC, abstractmethod
import orjson
import random
import itertools
import names
import csv
import os


class AbstractParser(ABC):
    """Abstract base class for parsers."""

    def __init__(self, file_type: str):
        self._write_strategy = self._write_jsonl if file_type == 'jsonl' else self._write_csv
        self.type_name = file_type
        self.users = []
        self.usersId = itertools.count(1)
        self.default_pfp = {
            "pfp_id": 1,
            "url": 'https://storage.cloud.google.com/data_warehousing_library_data/default-pfp.svg'
        }

    @abstractmethod
    def process_file(self, input_file: str, output: str) -> str:
        """
        Abstract method to process a file.

        Args:
            input_file: The input file path.
            output: The output file path.
        """
        
    def get_or_generate_reader(self) -> dict:
        if not self.users or random.random() < 20000/len(self.users)/random.randint(1,500):
            name = names.get_full_name()
            email = f'{name.replace(" ", "_")}@knyhozbirnia.com'
            newUser = {
                    "user_id": next(self.usersId),
                    "name": name,
                    "email": email,
                    "pfp_id": 1
                }
            self.users.append(newUser)
            return newUser
        else:
            return random.choice(self.users)

    @classmethod
    def _write_jsonl(cls, file: str, obj: dict)-> None:
        file.write(orjson.dumps(obj).decode('utf-8') + '\n')

    @classmethod
    def _write_csv(cls, file: str, obj: dict) -> None:
        # obj = {k: v.encode('unicode_escape').decode() if isinstance(v, str) else v for k, v in obj.items()}
        writer = csv.DictWriter(file, fieldnames=obj.keys(), quoting=csv.QUOTE_ALL)
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
