from abc import abstractmethod
import random
from .abstract_parser import AbstractParser


class OLAbstractParser(AbstractParser):
    """This is an abstract class for parsing Open Library files."""

    @abstractmethod
    def process_latest_file(self, directory: str) -> None:
        """
        Process the latest file in the specified directory.

        Parameters:
        - directory (str): The directory path where the files are located.

        Returns:
        None
        """
        
    @classmethod
    def parse_id(cls, key: str) -> str:
        """
        Parse the ID from the given key.

        Parameters:
        - key (str): The key from which to extract the ID.

        Returns:
        str: The extracted ID.
        """
        return key.split('/')[-1]

    @classmethod
    def get_random_time():
        hours = str(random.randint(0, 23)).zfill(2)
        minutes = str(random.randint(0, 59)).zfill(2)
        seconds = str(random.randint(0, 59)).zfill(2)
        return f'{hours}:{minutes}:{seconds}'
