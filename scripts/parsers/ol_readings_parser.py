from parsers.ol_abstract_parser import OLAbstractParser
from parsers.abstract_parser import AbstractParser
from parsers.user_manager import UserManager
from parsers.file_writer import FileWriter
from enum import Enum
import itertools
import glob
import os


class ReadingStatus(Enum):
    """
    Enum representing the reading status of a book.

    Attributes:
        WANT_TO_READ (str): Indicates that the book is in the "Want to Read" status.
        CURRENTLY_READING (str): Indicates that the book has "Currently Reading" status.
        ALREADY_READ (str): Indicates that the book is in the "Already Read" status.
    """

    WANT_TO_READ = 'Want to Read'
    CURRENTLY_READING = 'Currently Reading'
    ALREADY_READ = 'Already Read'


class OLReadingsParser(OLAbstractParser, FileWriter):
    """
    A class for parsing OL readings data.

    Inherits from OLAbstractParser.
    """

    def process_file(self, input_file: str, output_file: str) -> list[str]:
        """
        Process the input file and write the parsed data to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.

        Returns:
            list[str]: names of output files.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        with open(input_file, 'r', encoding='utf-8') as f_in, \
                open(output_file, 'w', encoding='utf-8', newline='') as f_out:
            for line in f_in:
                reading_info = self.__parse_line(line)
                for reading in reading_info:
                    self._write_strategy(f_out, reading)
        return [output_file]

    def process_latest_file(self, work_editions: dict, directory: str) -> list[str]:
        """
        Process the latest file in the specified directory.

        Args:
            directory (str): The path to the directory containing the files.
        """
        self.work_editions = work_editions

        files = glob.glob(os.path.join(directory, 'ol_dump_reading-log*.txt'))

        files.sort(reverse=True)

        return self.process_file(files[0], rf'{directory}\data\listing.{self.type_name}')

    def __parse_line(self, line: str) -> dict:
        """
        Parse a line of data and return the parsed information as a dictionary.

        Args:
            line (str): The line of data to parse.

        Returns:
            dict: A dictionary containing the parsed information.
        """
        try:
            fields = line.split('\t')
            shift = 1 if len(fields) == 4 else 0

            work_id = self.parse_id(fields[0])
            reading_status = ReadingStatus(fields[1 + shift]).name
            date = f"{fields[2 + shift].strip()}T{self.get_random_time()}"

            editions = self.work_editions.get(work_id, [])
            return [{
                'listing_id': next(self.listingId),
                'reader_id': self.user_manager.get_or_generate_reader(),
                'edition_id': edition,
                'reading_status': reading_status,
                'date': date,
            } for edition in editions]

        except Exception:
            pass
    
    def __init__(self, file_type: str, user_manager: UserManager) -> None:
        """
        Initializes an instance of the OLReadingsParser class.

        Args:
            file_type (str): The type of file to be parsed.
            user_manager (UserManager): An instance of the UserManager class.

        Returns:
            None
        """
        OLAbstractParser.__init__(self, user_manager)
        FileWriter.__init__(self, file_type)
        self.work_editions = []

        self.listingId = itertools.count(1)

