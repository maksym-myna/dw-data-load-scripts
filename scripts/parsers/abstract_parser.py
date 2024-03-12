from abc import ABC, abstractmethod
import os

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
    
    # def writeUser(self, user: dict):
    #     self._write_strategy(self.users_file, user)
                
    
    # @classmethod
    # def isbn10_to_isbn13(cls, isbn10: str) -> str:
    #     """
    #     Convert an ISBN-10 to an ISBN-13.

    #     Args:
    #         isbn10 (str): The ISBN-10 to be converted.

    #     Returns:
    #         str: The converted ISBN-13.
    #     """
    #     if len(isbn10) != 10:
    #         if len(isbn10) < 10:
    #             isbn10 = isbn10.zfill(10)
    #         else:
    #             raise ValueError(f'Invalid ISBN-10: {isbn10}')
        
    #     isbn13 = '978' + isbn10[:-1]
        
    #     check_digit = 0
    #     for i, digit in enumerate(isbn13):
    #         check_digit += int(digit) * (3 if i % 2 else 1)
    #     check_digit = (10 - (check_digit % 10)) % 10
        
    #     return isbn13 + str(check_digit)
    
    
    # @classmethod
    # def check_digit_10(cls, isbn):
    #     assert len(isbn) == 9
    #     sum = 0
    #     for i in range(len(isbn)):
    #         c = int(isbn[i])
    #         w = i + 1
    #         sum += w * c
    #     r = sum % 11
    #     if r == 10: return 'X'
    #     else: return str(r)

    # @classmethod
    # def check_digit_13(cls, isbn):
    #     assert len(isbn) == 12
    #     sum = 0
    #     for i in range(len(isbn)):
    #         c = int(isbn[i])
    #         if i % 2: w = 3
    #         else: w = 1
    #         sum += w * c
    #     r = 10 - (sum % 10)
    #     if r == 10: return '0'
    #     else: return str(r)

    # @classmethod
    # def convert_10_to_13(cls, isbn):
    #     assert len(isbn) == 10
    #     prefix = '978' + isbn[:-1]
    #     check = cls.check_digit_13(prefix)
    #     return prefix + check