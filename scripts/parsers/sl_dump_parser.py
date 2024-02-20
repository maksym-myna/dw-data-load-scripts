import orjson
from .abstract_parser import AbstractParser
from parsers.abstract_parser import AbstractParser


class SLDataParser(AbstractParser):
    """
    A parser for Seattle Library data files.

    This class inherits from the AbstractParser class and provides methods to
    process SL data files.
    """

    def process_file(self, input_file: str, output_file: str) -> list[str]:
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        with open(input_file, 'r', encoding='utf-8') as f_in, \
                open(output_file, 'w', encoding='utf-8', newline='') as f_out:
            for line in f_in:
                try:
                    line = line.replace('[', '').replace(']', '').replace(',{', '{')

                    data = orjson.loads(line)
                    data = self.__parse_line(data)

                    for row in data : 
                        self._write_strategy(f_out, row)
                except Exception as e:
                    continue
        return [output_file]

    @classmethod
    def __parse_line(cls, line: dict) -> dict:
        """
        Parse a line of JSON data and extract relevant information.

        Args:
            line (dict): The JSON data to be parsed.

        Returns:
            dict: A dictionary containing the extracted information.
        """
        checkoutyear = line.get('checkoutyear', 0)
        checkoutmonth = line.get('checkoutmonth', 0)
        checkouts = line.get('checkouts', 0)
        title = line.get('title')
        isbns = [isbn.strip() for isbn in line.get('isbn').split(',')]
                
        return [
            {
                'checkout_year': checkoutyear,
                'checkout_month': checkoutmonth,
                'checkouts': checkouts,
                'title': title,
                'isbn': isbn
            }
            for isbn in isbns
        ]
        
    def __init__(self, file_type) -> None:
        super().__init__(file_type)
                    