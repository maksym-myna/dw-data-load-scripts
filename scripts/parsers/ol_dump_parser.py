from string import punctuation, whitespace
from parsers.ol_abstract_parser import OLAbstractParser
from .abstract_parser import AbstractParser
from datetime import datetime
from dateutil.parser import parse as date_parser
import orjson
import html
import random
import itertools
import datetime
import os
import re

class OLDumpParser(OLAbstractParser): 
    """ A class for parsing Open Library dump files.
    Attributes:
    type_mapping (dict): Mapping type names to corresponding processing methods.
    """

    def process_file(self, input_file: str, output_file = None) -> list[str]:
        """
        Process a dump file and write the parsed data to output files.

        Args:
            input_file (str): The path to the input dump file.
            output_files (dict): A dictionary of output file objects.

        Returns:
            list[str]: names of output files.
        """
        if not AbstractParser.is_path_valid(input_file):
            raise NotADirectoryError(input_file)

        with open(input_file, 'r', encoding='utf-8') as f_in:
            for line in f_in:
                obj = orjson.loads(line.split('\t')[4]
                                #    .replace('\n',' ')
                                   )
                
                for key, value in obj.items():
                    if isinstance(value, str):
                        obj[key] = value.replace('\n', ' ').replace('\r', ' ')
                
                type_name = self.parse_id(obj.get('type')['key'])
                if type_name in self.__type_mapping:
                    try:
                        json_obj = self.__type_mapping[type_name](obj)
                        self._write_strategy(self.output_files[type_name], json_obj)
                    except Exception:
                        continue
        return self.output_files

    def process_latest_file(self, directory: str) -> list[str]:
        """
        Process the latest dump file in the given directory.

        Args:
            directory (str): The path to the directory containing dump files.

        Returns:
            None
        """
        pattern = pattern = re.compile(r'ol_dump_\d{4}-\d{2}-\d{2}.txt')

        # Get a list of all files in the directory that match the pattern
        files = (entry for entry in os.scandir(directory) if entry.is_file()
                    and pattern.match(entry.name))

        # Get the latest file
        latest_file = max(files, key=lambda f: f.name)

        if not AbstractParser.is_path_valid(directory):
            raise NotADirectoryError(directory)
    
        self.output_files = {type_name: open(os.path.join(directory,
                            rf'data\{type_name}.{self.type_name}'), 'w', encoding='utf-8', newline='')
                                    for type_name in self.__normalized_types}
        self.output_files = self.process_file(latest_file.path)

        
        for f_out in self.output_files.values():
            f_out.close()

        returnValue = [rf'open library dump\data\{type_name}.{self.type_name}'
                for type_name in self.__type_mapping]
        
        with open(rf'open library dump\data\pfp.{self.type_name}', 'w', encoding='utf-8') as f_in:
            self._write_strategy(f_in, self.default_pfp)

        with open(rf'open library dump\data\user.{self.type_name}', 'w', encoding='utf-8') as f_in:
            for user in self.users:
                self._write_strategy(f_in, user)

        return returnValue

    def __process_edition(self, obj: dict) -> dict:
        """
        Process an edition object and return a dictionary of parsed data.

        Args:
            obj (dict): The edition object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        created_datetime = date_parser(created)
        title = self.__html_escape(obj.get('title', ''))
        subtitle = self.__html_escape(obj.get('subtitle', ''))
        title = f'{title}: {subtitle}' if subtitle else title
        number_of_pages = obj.get('number_of_pages', 0)
        id = self.parse_id(obj.get('key', None))

        self.__print_normalized(id, 'edition_publisher', [self.__html_escape(publisher)
                    for publisher in obj.get('publishers', [])])
        self.__print_normalized(id, 'isbn_10', list(filter(lambda isbn: isbn.isdigit() and len(isbn)==10, obj.get('isbn_10', []))))
        self.__print_normalized(id, 'isbn_13', list(filter(lambda isbn: isbn.isdigit() and len(isbn)==13, obj.get('isbn_13', []))))
        self.__print_normalized(id, 'edition_series', list(map(lambda series: series.strip(punctuation+whitespace), obj.get('series', []))))
        self.__print_normalized(id, 'edition_language', [self.parse_id(lang['key']) for lang in obj.get('languages', [])])
        self.__print_normalized(id, 'edition_work', [self.parse_id(work['key']) for work in obj.get('works', [])])
        
        if not title or not id:
            raise Exception('No title found')  
        
        edition = {
            'id': id,
            'title': title,
            'number_of_pages': number_of_pages,
            'created': created,
        }
    
        for _ in range(0,random.choice(self.itemRandomList)):        
            self._write_strategy(self.output_files['inventory_item'], {
                'inventory_id': next(self.itemId),
                'edition_id': id,
                'added_at': created_datetime + datetime.timedelta(
                    days=random.randint(0, (datetime.datetime.now() - created_datetime).days)
                )
            })      
        return edition

    def __process_work(self, obj: dict) -> dict:
        """
        Process a work object and return a dictionary of parsed data.

        Args:
            obj (dict): The work object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
      
        title_prefix = obj.get('title_prefix', '')
        title = obj.get('title', '')
        subtitle = obj.get('subtitle', '')
        
        title = self.__html_escape(f'{title_prefix}. {title}: {subtitle}').strip(punctuation+whitespace)

        if not title:
            raise Exception('No title found')  
        
        id = self.parse_id(obj.get('key', None))
        self.__print_normalized(id, 'work_author', [self.parse_id(author['author']['key'])
                        for author in obj.get('authors', [])])
        self.__print_normalized(id, 'subject', [self.__html_escape(subject) for subject in obj.get('subjects', [])])
        return {
            'id': id,
            'title': title,
            'created': created,
        }

    def __process_author(self, obj: dict) -> dict:
        """
        Process an author object and return a dictionary of parsed data.

        Args:
            obj (dict): The author object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        id = self.parse_id(obj.get('key', None))
        name = self.__html_escape(obj.get('name', ''))
        if not name:
            raise Exception('No name found')  
        return {
            'id': id,
            'name': name,
            'created': created
        }

    def __process_language(self, obj: dict) -> dict:
        """
        Process a language object and return a dictionary of parsed data.

        Args:
            obj (dict): The language object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        id = self.parse_id(obj.get('key', None))
        name = obj.get('name', '')
        return {
            'id': id,
            'name': name,
            'created': created
        }

    @classmethod
    def __get_created(cls, obj: dict) -> str:
        """
        Get the created timestamp from an object.

        Args:
            obj (dict): The object from which to extract the created timestamp.

        Returns:
            str: The created timestamp.

        Raises:
            Exception: If no created or last_modified field is found.
        """
        created = obj.get('created', {}).get('value', '') \
            if obj.get('created') else obj.get('last_modified', {}).get('value', '') \
            if obj.get('last_modified') else datetime.now().isoformat()
        if not created:
            raise Exception('No created or last_modified field')
        return created

    @classmethod
    def __html_escape(cls, s: str) -> str:
        """
        Escapes HTML entities in a string.

        Args:
            s (str): The string to escape.

        Returns:
            str: The escaped string.
        """
        s = s.replace('&NewLine', ' ').rstrip("&#13").rstrip("&#10").replace('&#10;', '').replace('&#13;', '').replace('"', '').replace('&quot;', '')
        return html.unescape(s).rstrip('\\')
    
    def __print_normalized(self, id: str, name: str, obj: list[dict]) -> None:
        if (not obj):
            return
        for unit in [{ "id": id, name : unit } for unit in obj if unit]:
            self._write_strategy(self.output_files[name], unit)
        
    def __init__(self, file_type: str) -> None:
        super().__init__(file_type)
                
        self.__type_mapping = {
            'edition': self.__process_edition,
            'work': self.__process_work,
            'author': self.__process_author,
            'language': self.__process_language
        }
        
        self.__normalized_types = [
            'language',
            'edition',
            'edition_language',
            'edition_publisher',
            'isbn_10',
            'isbn_13',
            'edition_work',
            'edition_series',
            'work',
            'subject',
            'author',
            'work_author',
            'inventory_item'
        ]
        
        self.output_files = None
        self.itemId = itertools.count(1)
        self.itemRandomList = [0,0,0,0,0,0,0,0,0,0,0,1,1,2,2,3]
