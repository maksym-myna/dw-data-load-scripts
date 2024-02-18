from abc import ABC, abstractmethod 

class AbstractParser(ABC):
    """
    Abstract base class for parsers.
    """

    @abstractmethod
    def process_file(self, input, output):
        """
        Abstract method to process a file.

        Args:
            input: The input file path.
            output: The output file path.
        """
        pass