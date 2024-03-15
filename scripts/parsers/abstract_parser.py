from abc import ABC, abstractmethod
import os
import pyisbn

from parsers.user_manager import UserManager


class AbstractParser(ABC):
    """Abstract base class for parsers."""

    def __init__(self, user_manager: UserManager) -> None:
        self.user_manager = user_manager

    @abstractmethod
    def process_file(self, input_file: str, output: str) -> str:
        """
        Abstract method to process a file.

        Args:
            input_file: The input file path.
            output: The output file path.
        """

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

    @classmethod
    def convert_to_isbn13(cls, isbns: list) -> list:
        """
        Convert a list of ISBN-10s to ISBN-13s.

        Args:
            l (list): A list of ISBN-10s.

        Returns:
            list: A list of ISBN-13s.
        """
        validated_isbns = []
        for isbn in isbns:
            if len(isbn) != 13:
                isbn = ''.join(char for char in isbn if char.isdigit() or char == 'X').zfill(10)
                if len(isbn) == 10:
                    isbn = pyisbn.Isbn(isbn).convert()
                else:
                    continue
            validated_isbns.append(isbn)
        return validated_isbns

    @classmethod
    def _capitalize_first(cls, s: str) -> str:
        return s[:1].upper() + s[1:]