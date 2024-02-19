from abc import ABC, abstractmethod 
import os

class AbstractParser(ABC):
    """
    Abstract base class for parsers.
    """

    @abstractmethod
    def process_file(self, input_file, output):
        """
        Abstract method to process a file.

        Args:
            input_file: The input file path.
            output: The output file path.
        """
        
    @staticmethod
    def is_path_valid(path):
        return os.path.abspath('.') in os.path.abspath(path)
        