import random
from string import punctuation, whitespace
from parsers.ol_abstract_parser import OLAbstractParser
from parsers.file_writer import FileWriter
from parsers.user_manager import UserManager
from .abstract_parser import AbstractParser
from language_speakers import speakers
from datetime import datetime
from dateutil.parser import parse as date_parser
from concurrent.futures import ThreadPoolExecutor
import orjson
import html
import itertools
import os
import re

class OLDumpParser(OLAbstractParser, FileWriter): 
    """ A class for parsing Open Library dump files.
    Attributes:
    type_mapping (dict): Mapping type names to corresponding processing methods.
    """

    def __init__(self, file_type: str, user_manager: UserManager) -> None:
        """
        Initializes an instance of the OLDumpParser class.

        Args:
            file_type (str): The type of file being parsed.
            user_manager (UserManager): An instance of the UserManager class.

        Attributes:
            __type_mapping (dict): A dictionary mapping different types to their respective processing methods.
            __normalized_types (list): A list of normalized types.
            output_files (None): Placeholder for output file objects.
            language_file (file): The file object for the language file.
            edition_id (itertools.count): An iterator for generating unique edition IDs.
            item_id (itertools.count): An iterator for generating unique item IDs.
            publisher_id (itertools.count): An iterator for generating unique publisher IDs.
            subject_id (itertools.count): An iterator for generating unique subject IDs.
            work_authors (dict): A dictionary to store authors associated with works.
            work_subjects (dict): A dictionary to store subjects associated with works.
            work_editions (dict): A dictionary to store editions associated with works.
            publishers (dict): A dictionary to store publishers.
            subjects (dict): A dictionary to store subjects.
        """
        OLAbstractParser.__init__(self, user_manager)
        FileWriter.__init__(self, file_type)
        
        self.__type_mapping = {
            'edition': self.__process_edition,
            'work': self.__process_work,
            'author': self.__process_author,
            'language': self.__process_language
        }
        
        self.__normalized_types = [
            'language',
            "publisher",
            'edition',
            'edition_isbn'
            'edition_language',
            'edition_weight',
            'author',
            'edition_author',
            'subject',
            'edition_subject',
        ]
        
        self.output_files = None
        self.language_file = open(rf'open library dump\data\language.{self.type_name}', 'w', encoding='utf-8', newline='')

        self.edition_id = itertools.count(1)
        self.item_id = itertools.count(1)
        self.publisher_id = itertools.count(1)
        self.subject_id = itertools.count(1)
        
        self.work_authors = {}
        self.work_subjects = {}
        self.work_editions = {}
        self.publishers = {}
        self.subjects = {}
        

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
                obj = orjson.loads(line.split('\t')[4])
                
                for key, value in obj.items():
                    if isinstance(value, str):
                        obj[key] = value.replace('\n', ' ').replace('\r', ' ')
                
                type_name = self.parse_id(obj.get('type')['key'])
                if type_name in self.__type_mapping:
                    try:
                        json_obj = self.__type_mapping[type_name](obj)
                        self._write_strategy(self.output_files[type_name], json_obj)
                    except Exception as e:
                        continue
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(self.write_dictionary, self.publishers, 'publisher')
            executor.submit(self.write_dictionary, self.publishers, 'subject')
            executor.submit(self.write_work_dictionary, self.work_subjects, 'edition_subject')
            executor.submit(self.write_work_dictionary, self.work_authors, 'edition_author')  
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

        files = (entry for entry in os.scandir(directory) if entry.is_file()
                    and pattern.match(entry.name))

        latest_file = max(files, key=lambda f: f.name)

        if not AbstractParser.is_path_valid(directory):
            raise NotADirectoryError(directory)
    
        self.output_files = {type_name: open(os.path.join(directory,
                            rf'data\{type_name}.{self.type_name}'), 'w', encoding='utf-8', newline='')
                                    for type_name in self.__normalized_types}
        self.output_files = self.process_file(latest_file.path)
        
        self.user_manager.writePfp()

        for f_out in self.output_files.values():
            f_out.close()
            
        return [rf'open library dump\data\{type_name}.{self.type_name}'
                for type_name in self.__type_mapping]

    def __process_edition(self, obj: dict) -> dict:
        """
        Process an edition object and return a dictionary of parsed data.

        Args:
            obj (dict): The edition object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        title_prefix = obj.get('title_prefix', '')
        title = obj.get('title', '')
        subtitle = obj.get('subtitle', '')
        by_statement = obj.get('by_statement', '')
        
        title = self.__html_escape(f'{title_prefix}. {title}: {subtitle} {by_statement}').strip(punctuation+whitespace)

        if not title:
            raise Exception('No title found')  
    
        work_id = [self.parse_id(work['key']) for work in obj.get('works', ['"key": ""'])][0]
        edition_id = next(self.edition_id)

        isbns = obj.get('isbn_13', obj.get('isbn_10', []))
        isbns = self.convert_to_isbn13(isbns)
        self.__print_normalized(edition_id, 'edition_isbn', isbns)
            
        created = self.__get_created(obj)
        number_of_pages = obj.get('number_of_pages', 0)        

        publishers = [self.__html_escape(publisher) for publisher in obj.get('publishers', [])]
        publisher = 'Others' if not publishers else publishers[0]
        publisher_id = self.add_to_dictionary([publisher], self.publishers, self.publisher_id).get("ids", [0])[0]
    
        languages = [self.parse_id(lang['key']) for lang in obj.get('languages', [])]
        if languages:
            self.__print_normalized(edition_id, 'edition_language', [languages[0]])
    
        publish_date = obj.get('publish_date', None)
        year = self.find_year(publish_date) if publish_date else 0
        year = random.randint(1900, 2022) if year == 0 else year
                
        weight = obj.get('weight', None)
        if weight:
            kg_weight = self.find_weight_in_kg(weight)
            if kg_weight:
                self.__print_normalized(edition_id, 'edition_weight', [kg_weight])
        
        self.work_editions.setdefault(work_id, []).append(edition_id)
        
        return {
            'edition_id': edition_id,
            'publisher_id': publisher_id,
            'title': title,
            'number_of_pages': number_of_pages,
            'release_year': year,
            'created': created,
        }

    def __process_work(self, obj: dict) -> dict:
        """
        Process a work object and return a dictionary of parsed data.

        Args:
            obj (dict): The work object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        title_prefix = obj.get('title_prefix', '')
        title = obj.get('title', '')
        subtitle = obj.get('subtitle', '')
        title = self.__html_escape(f'{title_prefix}. {title}: {subtitle}').strip(punctuation+whitespace)

        if not title:
            raise Exception('No title found')  
        
        id = self.parse_id(obj.get('key', None))
        
        works_authors = [self.parse_id(author['author']['key']) for author in obj.get('authors', [])]
        
        subject = [self.__html_escape(subject) for subject in obj.get('subjects', [])]
        works_subjects = self.add_to_dictionary(subject, self.subjects, self.subject_id).get("ids", [])

        self.work_authors[id] = works_authors
        self.work_subjects[id] = works_subjects
        
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
        
        language = {
            'id': id,
            'name': name,
            'speakers': speakers.get(id, 0),
            'added_at': created,
        }
        
        self._write_strategy(self.language_file, language)
        self.language_file.flush()

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
            """
            Prints the normalized data for a given id, name, and object.

            Args:
                id (str): The identifier for the data.
                name (str): The name of the data.
                obj (list[dict]): The list of dictionaries containing the data.

            Returns:
                None
            """
            if (not obj):
                return
            for unit in [{ "id": id, name : unit } for unit in obj if unit]:
                self._write_strategy(self.output_files[name], unit)
            
    @classmethod
    def find_year(cls, s: str) -> int:
        """
        Finds the first occurrence of a 4-digit year in a given string.

        Args:
            s (str): The input string to search for a year.

        Returns:
            int: The found year if it is less than or equal to the current year, otherwise 0.
        """
        current_year = datetime.now().year
        pattern = re.compile(r'\b\d{4}\b')
        for match in pattern.finditer(s):
            year = int(match.group())
            if year <= current_year:
                return year
        return 0
        
    @classmethod
    def find_weight_in_kg(cls, s: str) -> float:
        """
        Finds the weight in kilograms from a given string.

        Args:
            s (str): The input string.

        Returns:
            float: The weight in kilograms, rounded to 2 decimal places.
                    Returns None if the weight cannot be found.
        """
        pattern = re.compile(r'\d+(\.\d+)?')
        result = 0
        if 'k' in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) if result else None
        elif 'g' in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 1000 if result else None
        elif 'z' in s or 'ounc' in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 35.284 if result else None
        elif 'lb' in s or 'pound' in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 2.205 if result else None
        return round(result, 2) if result else None
    
    def add_to_dictionary(self, l: list, dictionary: dict, itertools_id) -> None:
        """
        Adds items from a list to a dictionary, assigning unique IDs to each item.

        Args:
            l (list): The list of items to be added to the dictionary.
            dictionary (dict): The dictionary to which the items will be added.
            itertools_id: An iterator that generates unique IDs.

        Returns:
            dict: A dictionary containing the assigned IDs and the items added.

        """
        ids = []
        items = []
        for item in l:
            id = 0
            if item not in dictionary:
                id = next(itertools_id)
                dictionary[item] = id
                items.append({
                    "id" : id,
                    "item" : item
                    }
                )
            else:
                id = dictionary[item]
            ids.append(id)
        return {
            "ids" : ids,
            "items" : items
        }
        
    def write_dictionary(self, dictionary: dict, name: str) -> None:
        """
        Writes the given dictionary to the output file with the specified name.

        Args:
            dictionary (dict): The dictionary to be written.
            name (str): The name of the output file.

        Returns:
            None
        """
        for key, value in dictionary.items():
            self._write_strategy(self.output_files[name], {
            "id" : value,
            name : key
        })
        dictionary = []
        
    def write_work_dictionary(self, dictionary: dict, name: str) -> None:
        """
        Writes the given work dictionary to the output file.

        Args:
            dictionary (dict): The work dictionary containing work IDs as keys and authors as values.
            name (str): The name of the output file.

        Returns:
            None
        """
        for key, value in dictionary.items():
            editions = self.work_editions.get(key, [])
            for edition in editions:
                for author in value:
                    self._write_strategy(self.output_files[name], {
                    "id" : edition,
                    name : author
                })
        dictionary = []
