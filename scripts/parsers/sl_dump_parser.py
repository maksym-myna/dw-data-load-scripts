import orjson
from .abstract_parser import AbstractParser

# https://data.seattle.gov/resource/tmmm-ytt6.json?$query=SELECT%20`materialtype`,%20`checkoutyear`,%20`checkoutmonth`,%20`checkouts`,%20`title`,%20`isbn`%20WHERE%20(`isbn`%20IS%20NOT%20NULL)%20AND%20caseless_one_of(%20`materialtype`,%20%22BOOK,%20ER%22,%20%22BOOK%22,%20%22AUDIOBOOK%22,%20%22EBOOK%22%20)%20ORDER%20BY%20`title`%20DESC%20NULL%20LAST,%20`isbn`%20DESC%20NULL%20LAST%20LIMIT%202147483647

class SLDataParser(AbstractParser):
    """
    A parser for Seattle Library data files.

    This class inherits from the AbstractParser class and provides methods to process SL data files.
    """

    def process_file(self, input_file, output_file):
        """
        Process the input file and write the modified JSON objects to the output file.

        Args:
            input_file (str): The path to the input file.
            output_file (str): The path to the output file.
        """
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                try:
                    line = line.replace('[', '').replace(']', '').replace(',{', '{')

                    data = orjson.loads(line)
                    data = self.__parse_line(data)
                    
                    f_out.write(orjson.dumps(data).decode('utf-8') +'\n')
                except Exception as e:
                    print(e)
                    continue
        return [output_file]
            
    def __parse_line(self, line):
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
        isbns = [isbn.strip() for isbn in line.get('isbn').split(',')]
        title = line.get('title')
        
        return {
            'checkout_year': checkoutyear,
            'checkout_month': checkoutmonth,
            'checkouts': checkouts,
            'isbn': isbns,
            'title': title
        }