from parsers.ol_abstract_parser import OLAbstractParser
from parsers.abstract_parser import AbstractParser
from enum import Enum
import orjson
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


class OLReadingsParser(OLAbstractParser):
    """
    A class for parsing OL readings data.

    Inherits from OLAbstractParser.
    """

    def process_file(self, input_file, output_file):
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

        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                reading_info = self.__parse_line(line)
                f_out.write(orjson.dumps(reading_info).decode('utf-8') + '\n')  # Write the JSON object followed by a newline
        return [output_file]

    def process_latest_file(self, directory):
        """
        Process the latest file in the specified directory.

        Args:
            directory (str): The path to the directory containing the files.
        """
        # Get a list of all files in the directory that match the pattern
        files = glob.glob(os.path.join(directory, 'ol_dump_reading-log*.txt'))

        files.sort(reverse=True)
        return self.process_file(files[0], rf'{directory}\data\readings.jsonl')

    def __parse_line(self, line):
        """
        Parse a line of data and return the parsed information as a dictionary.

        Args:
            line (str): The line of data to parse.

        Returns:
            dict: A dictionary containing the parsed information.
        """
        fields = line.split('\t')
        shift = 1 if len(fields) == 4 else 0

        work = self.parse_id(fields[0])
        reading_status = ReadingStatus(fields[1 + shift]).name
        date = fields[2 + shift].strip()

        return {
            'work': work,
            'reading_status': reading_status,
            'date': date
        }
