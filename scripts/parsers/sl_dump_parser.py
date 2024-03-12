from io import TextIOWrapper
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

    def process_file(self, input_file: str, output_files: list[str] ) -> list[str]:
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)
        location = output_files[1].rpartition('\\')[0]
        with open(input_file, 'r', encoding='utf-8') as f_in, \
                open(output_files[0], 'w', encoding='utf-8', newline='') as loan_out, \
                    open(output_files[1], 'w', encoding='utf-8', newline='') as return_out, \
                        open(location + '\\inventory_item.csv', 'w', encoding='utf-8', newline='') as item_out, \
                            open(location + '\\return.csv', 'w', encoding='utf-8', newline='') as return_out:
            for line in f_in:
                try:
                    line = line.replace('[', '').replace(']', '').replace(',{', '{').replace(r'\\\\',r'\\')

                    data = orjson.loads(line)
                    data = self.__parse_file(data)
                except Exception:
                    continue
                
            self.process_data(item_out, loan_out, return_out)    
        self.clear_up()
        return output_files

    def __parse_file(self, line: dict) -> None:
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
        material_type = line.get('materialtype', None)

        split_isbns = [isbn.strip(" '") for isbn in line.get('isbn').split(',')]
        isbn = list(filter(lambda isbn: isbn, split_isbns))[0]        
        isbn = self.isbn10_to_isbn13(isbn) if len(isbn) == 10 else isbn
        
        self.loans.append({
            "checkout_year" : checkoutyear,
            "checkout_month" : checkoutmonth,
            "checkouts" : checkouts,
            "isbn": isbn
        })
        
        self.items_maxxing[isbn] = {
                "qty": max(self.items_maxxing.get(isbn, 0), checkouts) if material_type == 'BOOK' else 1,
                "material_type": material_type
            }

    def process_data(self, item_out: TextIOWrapper, loan_out: TextIOWrapper, return_out: TextIOWrapper):
        items_ids = {}
        for key in list(self.items_maxxing.keys()):
            item = self.items_maxxing[key]
            qty = item.get("qty")
            material_type = item.get("material_type")
            
            checkouts = qty - random.randint(-2, 2) if qty > 5 else qty - random.randint(-1, 1) if qty > 2 else qty

            start_datetime = datetime(2010, 1, 1)
            ids=[]
            for _ in range(0, checkouts):
                id = next(self.inventoryId)
                ids.append(id)
                self._write_strategy(item_out, {
                    'inventory_id': id,
                    'isbn': key,
                    # 'condition' : random.randint(10,100),
                    'material_type' : material_type,
                    'added_at': start_datetime + timedelta(
                    days=random.randint(0, (datetime.now() - start_datetime).days))
            })
            items_ids[key] = ids
            ids = []
            del self.items_maxxing[key]
        
        for item in self.loans:
            for i in range(0, item.get("checkouts")):                
                checkoutyear = item.get("checkout_year")
                checkoutmonth = item.get("checkout_month")
                isbn = item.get("isbn")
                ids = items_ids.get(isbn, [])
                random.shuffle(ids)
                
                loan = {
                    'loan_id': next(self.loanId),
                    'user_id': self.user_manager.get_or_generate_reader(),
                    'inventory_id': ids[i%len(ids)],
                    'loaned_at': datetime(
                        checkoutyear,
                        checkoutmonth,
                        random.randint(1, calendar.monthrange(checkoutyear, checkoutmonth)[1]),
                        random.randint(0, 23),
                        random.randint(0, 59),
                        random.randint(0, 59)
                    ),
                }
                    
                loan_return = {
                    "loan_id": loan["loan_id"],
                    "return_date": loan['loaned_at'] + timedelta(days=random.randint(1, 14)) 
                }      
                
                self._write_strategy(loan_out, loan)              
                self._write_strategy(return_out, loan_return)
                
    def clear_up(self):
        self.loans = []
        self.items_maxxing = {}
    
    def __init__(self, file_type: str, user_manager: UserManager) -> None:
        AbstractParser.__init__(self, user_manager)
        FileWriter.__init__(self, file_type)
                
        self.loans = []
        self.items_maxxing = {}
        
        self.loanId = itertools.count(1)
        self.inventoryId = itertools.count(1)