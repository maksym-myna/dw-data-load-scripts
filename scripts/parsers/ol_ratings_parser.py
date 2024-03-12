from io import TextIOWrapper
from parsers.ol_abstract_parser import OLAbstractParser
from parsers.abstract_parser import AbstractParser
import os
import glob
import itertools

from parsers.user_manager import UserManager
from parsers.file_writer import FileWriter

class OLRatingsParser(OLAbstractParser, FileWriter):
    """
    A class for parsing Open Library ratings data.

    Attributes:
        None

    Methods:
        process_file(input_file, output_file): Process the input file and
            write the parsed data to the output file.
        process_latest_file_jsonl(directory): Process the latest ratings file in
            the specified directory.
        parse_line(line): Parse a single line of ratings data.

    Returns:
        list[str]: names of output files.
    """

    def process_file(self, input_file: str, output_file: str) -> list[str]:
        """
        Process the input file and write the parsed data to the output file.

        Args:
            input_file (str): Path to the input file.
            output_file (str): Path to the output file.

        Returns:
            list[str]: Names of output files.
        """

        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        with open(input_file, 'r', encoding='utf-8') as f_in, \
                open(output_file, 'w', encoding='utf-8', newline='') as f_out:
            for line in f_in:
                rating_info = self.__parse_line(line)
                for rating in rating_info:
                    self._write_strategy(f_out, rating)
        return [output_file]

    def process_latest_file(self, work_editions: dict, directory: str) -> list[str]:
        """
        Process the latest ratings file in the specified directory.

        Args:
            directory (str): Path to the directory containing the ratings files.

        Returns:
            list[str]: Names of output files.
        """
        self.work_editions = work_editions
        files = glob.glob(os.path.join(directory, 'ol_dump_ratings*.txt'))

        files.sort(reverse=True)
        return self.process_file(files[0], rf'{directory}\data\rating.{self.type_name}')

    def __parse_line(self, line: str) -> dict:
        """
        Parse a single line of ratings data.

        Args:
            line (str): Line of ratings data.

        Returns:
            dict: Parsed data as a dictionary.
        """
        fields = line.split('\t')
        shift = 1 if len(fields) == 4 else 0
        work_id = fields[0].split('/')[-1]
        rating = int(fields[1 + shift])
        date = f"{fields[2 + shift].strip()}T{self.get_random_time()}"
        
        editions = self.work_editions.get(work_id, [])
        return [{
            "rating_id": next(self.ratingId),
            'reader_id': self.user_manager.get_or_generate_reader(),
            'edition_id': edition,
            'rating': rating,
            'date': date,
        } for edition in editions]
        
    def __init__(self, file_type: str, user_manager: UserManager) -> None:
        """
        Initializes an instance of the OL Ratings Parser.

        Args:
            file_type (str): The type of file being parsed.
            user_manager (UserManager): An instance of the UserManager class.

        Returns:
            None
        """
        OLAbstractParser.__init__(self, user_manager)
        FileWriter.__init__(self, file_type)
        self.work_editions = []
        self.ratingId = itertools.count(1)
        
