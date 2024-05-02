from .abstract_parser import AbstractParser
from .user_manager import UserManager
from .abstract_parser import AbstractParser
from .file_writer import FileWriter

from datetime import datetime, timedelta
from io import TextIOWrapper

import itertools
import calendar
import sqlite3
import orjson
import random
import os


class SLDataParser(AbstractParser, FileWriter):
    """
    A parser for Seattle Library data files.

    This class inherits from the AbstractParser class and provides methods to
    process SL data files.
    """

    def __init__(
        self, conn: sqlite3.Connection, file_type: str, user_manager: UserManager
    ) -> None:
        """
        Initialize the SLDumpParser object.

        Args:
            file_type (str): The type of file being parsed.
            user_manager (UserManager): An instance of the UserManager class.

        Returns:
            None
        """
        AbstractParser.__init__(self, user_manager, conn)
        FileWriter.__init__(self, file_type)

        self.__loans = []
        self.__items_maxxing = {}
        self.__work_isbns = {}

        self.__loan_id = itertools.count(1)
        self.__inventory_id = itertools.count(1)

    def process_file(self, input_file: str, output_files: list[str]) -> list[str]:
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        self.__load_work_ids()
        CHUNK_SIZE = 1000

        directory = output_files[1].rpartition("\\")[0]
        item_out_location = directory + f"\\inventory_item.{self.type_name}"
        os.makedirs(rf"{directory}", exist_ok=True)
        with open(input_file, "r", encoding="utf-8") as f_in, open(
            output_files[0], "w", encoding="utf-8", newline=""
        ) as loan_out, open(
            output_files[1], "w", encoding="utf-8", newline=""
        ) as return_out, open(
            item_out_location, "w", encoding="utf-8", newline=""
        ) as item_out:
            print(f"Reading file '{input_file}'- {datetime.now().isoformat()}")
            while True:
                if not (lines := list(itertools.islice(f_in, CHUNK_SIZE))):
                    break
                for line in lines:
                    try:
                        line = (
                            line.replace("[", "")
                            .replace("]", "")
                            .replace(",{", "{")
                            .replace(r"\\\\", r"\\")
                        )

                        data = orjson.loads(line)
                        self.__parse_line(data)
                    except Exception:
                        continue
            self.process_data(item_out, loan_out, return_out)
        self.clear_up()
        return [item_out_location] + output_files

    def __parse_line(self, line: dict) -> None:
        """
        Parse a line of JSON data and extract relevant information.

        Args:
            line (dict): The JSON data to be parsed.

        Returns:
            dict: A dictionary containing the extracted information.
        """
        checkoutyear = int(line.get("checkoutyear", 0))
        checkoutmonth = int(line.get("checkoutmonth", 0))
        checkouts = int(line.get("checkouts", 0))
        material_type = line.get("materialtype", None)

        split_isbns = [isbn.strip(" '") for isbn in line.get("isbn").split(",")]
        isbns = self.convert_to_isbn13(split_isbns)

        for isbn in isbns:
            if work_id := self.__work_isbns.get(isbn):
                break
        else:
            return

        self.__loans.append(
            {
                "checkout_year": checkoutyear,
                "checkout_month": checkoutmonth,
                "checkouts": checkouts,
                "work_id": work_id,
            }
        )

        self.__items_maxxing[work_id] = {
            "qty": (
                max(
                    self.__items_maxxing.get(work_id, {}).get("qty", 0),
                    checkouts,
                )
                if material_type == "BOOK"
                else 1
            ),
            "material_type": material_type,
        }

    def process_data(
        self,
        item_out: TextIOWrapper,
        loan_out: TextIOWrapper,
        return_out: TextIOWrapper,
    ):
        """
        Process data to generate items, loans, and loan returns.

        Args:
            item_out (TextIOWrapper): Output file for items.
            loan_out (TextIOWrapper): Output file for loans.
            return_out (TextIOWrapper): Output file for loan returns.
        """
        CHUNK_SIZE = 1001

        items = []
        loans = []
        returns = []
        items_ids = {}
        for work_id in list(self.__items_maxxing.keys()):
            item = self.__items_maxxing[work_id]
            qty = item.get("qty")
            material_type = item.get("material_type")

            checkouts = (
                qty - random.randint(-2, 2)
                if qty > 5
                else qty - random.randint(-1, 1) if qty > 2 else qty
            )

            ids = []
            for _ in range(0, checkouts):
                if not (id := next(self.__inventory_id)):
                    return None
                ids.append(id)
                items.append((id, work_id, material_type))

            items_ids[work_id] = ids
            ids = []
            del self.__items_maxxing[work_id]

        for item in self.__loans:
            checkoutyear = item.get("checkout_year")
            checkoutmonth = item.get("checkout_month")
            work_id = item.get("work_id")
            ids = items_ids[work_id]

            for i in range(0, item.get("checkouts")):
                random.shuffle(ids)

                loaned_at = datetime(
                    checkoutyear,
                    checkoutmonth,
                    random.randint(
                        1, calendar.monthrange(checkoutyear, checkoutmonth)[1]
                    ),
                    random.randint(0, 23),
                    random.randint(0, 59),
                    random.randint(0, 59),
                    random.randint(0, 999999),
                )
                if not (loan_id := next(self.__loan_id)):
                    return None

                loan = (
                    loan_id,
                    self.user_manager.get_or_generate_reader(),
                    ids[i % len(ids)],
                    loaned_at.isoformat(),
                )

                loan_return = (
                    loan[0],
                    (
                        loaned_at
                        + timedelta(
                            days=random.randint(1, 14),
                            hours=random.randint(0, 23),
                            minutes=random.randint(0, 59),
                        )
                    ).isoformat(),
                )

                loans.append(loan)
                returns.append(loan_return)

                if len(loans) == CHUNK_SIZE:
                    self.__write_to_files(loan_out, item_out, return_out, loans, items, returns)
                    loans.clear()
                    items.clear()
                    returns.clear()
        self.__write_to_files(loan_out, item_out, return_out, loans, items, returns)

    def __write_to_files(self, loan_out, item_out, return_out, loans, items, returns):
        self._tuple_write_strategy(loan_out, loans)
        self._tuple_write_strategy(item_out, items)
        self._tuple_write_strategy(return_out, returns)

    def clear_up(self):
        """
        Clears the loans list and resets the items_maxxing dictionary.
        """
        self.__loans = []
        self.__items_maxxing = {}
        self.cursor.close()

    def __load_work_ids(self):
        self.cursor.execute("SELECT isbn, work_isbn.work_id FROM work_isbn join work_id on work_isbn.work_id = work_id.work_id")
        self.__work_isbns = {row[0]: row[1] for row in self.cursor.fetchall()}
