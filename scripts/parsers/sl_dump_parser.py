from io import TextIOWrapper
import os
import sqlite3
import orjson
import datetime
import random
import calendar
import itertools

from parsers.user_manager import UserManager
from .abstract_parser import AbstractParser
from parsers.abstract_parser import AbstractParser
from parsers.file_writer import FileWriter
from datetime import datetime, timedelta


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

        self.loans = []
        self.items_maxxing = {}
        self.work_isbns = {}

        self.loanId = itertools.count(1)
        self.inventoryId = itertools.count(1)

    def process_file(self, input_file: str, output_files: list[str]) -> list[str]:
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        directory = output_files[1].rpartition("\\")[0]
        item_out_location = directory + f"\\inventory_item.{self.type_name}"
        os.makedirs(f"{directory}\data", exist_ok=True)
        with open(input_file, "r", encoding="utf-8") as f_in, open(
            output_files[0], "w", encoding="utf-8", newline=""
        ) as loan_out, open(
            output_files[1], "w", encoding="utf-8", newline=""
        ) as return_out, open(
            item_out_location, "w", encoding="utf-8", newline=""
        ) as item_out:
            for line in f_in:
                try:
                    line = (
                        line.replace("[", "")
                        .replace("]", "")
                        .replace(",{", "{")
                        .replace(r"\\\\", r"\\")
                    )

                    data = orjson.loads(line)
                    self.__parse_file(data)
                except Exception:
                    continue

            self.process_data(item_out, loan_out, return_out)
        self.clear_up()
        return [item_out_location] + output_files

    def __parse_file(self, line: dict) -> None:
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

        work_id = None
        for isbn in isbns:
            self.cursor.execute("SELECT work_id FROM work_isbn WHERE isbn = ?", (isbn,))
            if result := self.cursor.fetchone():
                work_id = result[0]
        if not work_id:
            return

        self.loans.append(
            {
                "checkout_year": checkoutyear,
                "checkout_month": checkoutmonth,
                "checkouts": checkouts,
                "work_id": work_id,
            }
        )

        self.items_maxxing[work_id] = {
            "qty": (
                max(
                    self.items_maxxing.get(work_id, {"qty": 0}).get("qty", 0), checkouts
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
        items_ids = {}
        for key in list(self.items_maxxing.keys()):
            item = self.items_maxxing[key]
            qty = item.get("qty")
            material_type = item.get("material_type")

            checkouts = (
                qty - random.randint(-2, 2)
                if qty > 5
                else qty - random.randint(-1, 1) if qty > 2 else qty
            )

            start_datetime = datetime(2010, 1, 1)
            ids = []
            for _ in range(0, checkouts):
                if not (id := next(self.inventoryId)):
                    return None

                ids.append(id)
                self._write_strategy(
                    item_out,
                    {
                        "inventory_id": id,
                        "work_id": key,
                        "material_type": material_type,
                        "added_at": (
                            start_datetime
                            + timedelta(
                                days=random.randint(
                                    0, (datetime.now() - start_datetime).days
                                )
                            )
                        ).isoformat(),
                    },
                )
            items_ids[key] = ids
            ids = []
            del self.items_maxxing[key]

        for item in self.loans:
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
                if not (loan_id := next(self.loanId)):
                    return None

                loan = {
                    "loan_id": loan_id,
                    "user_id": self.user_manager.get_or_generate_reader(),
                    "inventory_id": ids[i % len(ids)],
                    "loaned_at": loaned_at.isoformat(),
                }

                loan_return = {
                    "loan_id": loan["loan_id"],
                    "return_date": (
                        loaned_at + timedelta(days=random.randint(1, 14))
                    ).isoformat(),
                }

                self._write_strategy(loan_out, loan)
                self._write_strategy(return_out, loan_return)

    def clear_up(self):
        """
        Clears the loans list and resets the items_maxxing dictionary.
        """
        self.loans = []
        self.items_maxxing = {}
        self.cursor.close()
