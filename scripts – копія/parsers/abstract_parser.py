from abc import ABC, abstractmethod
import os
import sqlite3
import pyisbn

from .user_manager import UserManager


class AbstractParser(ABC):
    """Abstract base class for parsers."""

    def __init__(self, user_manager: UserManager, conn: sqlite3.Connection) -> None:
        self.user_manager = user_manager
        self.conn = conn
        self.cursor = self.conn.cursor()

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
        return os.path.abspath(".") in os.path.abspath(path)

    @staticmethod
    def convert_to_isbn13(isbns: list) -> list:
        """
        Convert a list of ISBN-10s to ISBN-13s.

        Args:
            l (list): A list of ISBN-10s.

        Returns:
            list: A list of ISBN-13s.
        """
        return [
            (
                pyisbn.Isbn(
                    "".join([char for char in isbn if char.isdigit() or char == "X"])
                ).convert()
                if len(isbn) == 10
                else isbn
            )
            for isbn in isbns
            if len(isbn) in [10, 13]
        ]

    @staticmethod
    def capitalize_first(s: str) -> str:
        """
        Capitalizes the first character of a string.

        Args:
            s (str): The input string.

        Returns:
            str: The string with the first character capitalized.
        """
        return s[:1].upper() + s[1:]
