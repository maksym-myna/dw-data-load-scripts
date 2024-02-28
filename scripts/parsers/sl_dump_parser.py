import orjson
import datetime
import random
import calendar
import itertools
from .abstract_parser import AbstractParser
from parsers.abstract_parser import AbstractParser


class SLDataParser(AbstractParser):
    """
    A parser for Seattle Library data files.

    This class inherits from the AbstractParser class and provides methods to
    process SL data files.
    """

    def process_file(self, input_file: str, output_files: list[str] ) -> list[str]:
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        with open(input_file, 'r', encoding='utf-8') as f_in, \
                open(output_files[0], 'w', encoding='utf-8', newline='') as loan_out, \
                    open(output_files[1], 'w', encoding='utf-8', newline='') as return_out:
            for line in f_in:
                try:
                    line = line.replace('[', '').replace(']', '').replace(',{', '{').replace(r'\\\\',r'\\')

                    data = orjson.loads(line)
                    data = self.__parse_line(data)

                    for row in data : 
                        self._write_strategy(loan_out, row)
                        if random.random() < 0.25:
                            self._write_strategy(return_out, {
                                'return_id': next(self.returnId),
                                'loan_id': row.get('loan_id'),
                                'returned_at': row.get('loaned_at') + datetime.timedelta(days=random.randint(1, 30))
                            })
                except Exception:
                    continue
            
        return output_files

    def __parse_line(self, line: dict) -> dict:
        """
        Parse a line of JSON data and extract relevant information.

        Args:
            line (dict): The JSON data to be parsed.

        Returns:
            dict: A dictionary containing the extracted information.
        """
        checkoutyear = int(line.get('checkoutyear', 0))
        checkoutmonth = int(line.get('checkoutmonth', 0))
        checkouts = int(line.get('checkouts', 0))
        
        split_isbns = [isbn.strip(" '") for isbn in line.get('isbn').split(',')]
        isbns = list(filter(lambda isbn: isbn, split_isbns))
                
        return [
            {
                'loan_id': next(self.loanId),
                'user_id': self.get_or_generate_reader().get('user_id'),
                'loaned_at': datetime.datetime(
                    checkoutyear,
                    checkoutmonth,
                    random.randint(1, calendar.monthrange(checkoutyear, checkoutmonth)[1]),
                    random.randint(0, 23),
                    random.randint(0, 59),
                    random.randint(0, 59)
                ),
                'isbn': isbn
            }
            for isbn in isbns
            for _ in range(checkouts)
        ]
        
    def __init__(self, file_type) -> None:
        super().__init__(file_type)
        self.loanId = itertools.count(1)
        self.returnId = itertools.count(1)
                    