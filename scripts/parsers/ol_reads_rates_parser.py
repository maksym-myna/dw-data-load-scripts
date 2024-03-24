import sqlite3
from parsers.ol_abstract_parser import OLAbstractParser
from parsers.abstract_parser import AbstractParser
from parsers.user_manager import UserManager
from parsers.file_writer import FileWriter
from enum import Enum
from typing import Literal
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

    WANT_TO_READ = "Want to Read"
    CURRENTLY_READING = "Currently Reading"
    ALREADY_READ = "Already Read"


class OLRRParser(OLAbstractParser, FileWriter):
    """
    A class for parsing OL readings data.

    Inherits from OLAbstractParser.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        file_type: str,
        user_manager: UserManager,
        strategy: Literal["listing", "rating"],
    ) -> None:
        """
        Initialize the OLReadsRatesParser object.

        Parameters:
        conn (sqlite3.Connection): The SQLite database connection.
        file_type (str): The type of file to be written.
        strategy (str): The strategy to be used for parsing.
        user_manager (UserManager): The user manager object.

        Returns:
        None
        """
        OLAbstractParser.__init__(self, user_manager, conn)
        FileWriter.__init__(self, file_type)

        self.__id = itertools.count(1)
        self.__work_ids = {}

        self.strategy_name = strategy
        if strategy == "listing":
            self.__input_file_name = "reading-log"
            self.__field_strategy = self.readings_field_strategy
        elif strategy == "rating":
            self.__input_file_name = "ratings"
            self.__field_strategy = self.ratings_field_strategy
        else:
            raise ValueError("Invalid strategy")

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

        with open(input_file, "r", encoding="utf-8") as f_in, open(
            output_file, "w", encoding="utf-8", newline=""
        ) as f_out:
            for line in f_in:
                if result := self.__parse_line(line):
                    self._write_strategy(f_out, result)

        return output_file

    def process_latest_file(
        self, directory: str, work_ids: dict[str, int]
    ) -> list[str]:
        """
        Process the latest file in the specified directory.

        Args:
            directory (str): The path to the directory containing the files.
        """
        self.__work_ids = work_ids

        files = glob.glob(
            os.path.join(directory, f"ol_dump_{self.__input_file_name}*.txt")
        )

        files.sort(reverse=True)

        return self.process_file(
            files[0], rf"{directory}\data\{self.strategy_name}.{self.type_name}"
        )

    def __parse_line(self, line: str) -> dict:
        """
        Parse a single line of ratings data.

        Args:
            line (str): Line of ratings data.

        Returns:
            dict: Parsed data as a dictionary.
        """
        fields = line.split("\t")
        shift = 1 if len(fields) == 4 else 0

        work_id = fields[0].split("/")[-1]
        if not (work_id := self.__get_work_id(work_id)):
            return None

        field = self.__field_strategy(fields, shift)
        date = f"{fields[2 + shift].strip()}T{self.get_random_time()}"

        return (
            None
            if not (id := next(self.__id))
            else {
                "id": id,
                "reader_id": self.user_manager.get_or_generate_reader(),
                "work_id": work_id,
                "value": field,
                "date": date,
            }
        )

    def ratings_field_strategy(self, fields: list[str], shift: int):
        return int(fields[1 + shift])

    def readings_field_strategy(self, fields: list[str], shift: int):
        return ReadingStatus(fields[1 + shift]).name

    def __get_work_id(self, old_id: str):
        """
        Retrieves the work ID associated with the given old ID.

        Args:
            old_id (str): The old ID to search for.

        Returns:
            str or None: The work ID if found, None otherwise.
        """
        if work_id := self.__work_ids.get(old_id, None):
            self.cursor.execute(
                f"SELECT work_id FROM work_id WHERE work_id = (?)", (work_id,)
            )

        return work_id if work_id and self.cursor.fetchone() else None
