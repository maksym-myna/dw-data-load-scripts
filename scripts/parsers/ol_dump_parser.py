import io
import random
from string import punctuation, whitespace
from parsers.ol_abstract_parser import OLAbstractParser
from parsers.file_writer import FileWriter
from parsers.user_manager import UserManager
from .abstract_parser import AbstractParser
from language_speakers import speakers
from datetime import datetime
import orjson
import html
import itertools
import os
import re
import sqlite3

class OLDumpParser(OLAbstractParser, FileWriter):
    """ 
    A class for parsing Open Library dump files.
    
    Attributes:
        type_mapping (dict): Mapping type names to corresponding processing methods.
    """

    def __init__(self, conn: sqlite3.Connection, file_type: str, user_manager: UserManager) -> None:
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
            publisher_id (itertools.count): An iterator for generating unique publisher IDs.
            subject_id (itertools.count): An iterator for generating unique subject IDs.
            publishers (dict): A dictionary to store publishers.
            subjects (dict): A dictionary to store subjects.
        """
        OLAbstractParser.__init__(self, user_manager)
        FileWriter.__init__(self, file_type)

        self.__type_mapping = {
            'edition': self.__process_edition,
            'work': self.__process_work,
            'author': self.__process_author,
            'language': self.__process_language,
            'subject': self.__process_subject,
        }

        self.__normalized_types = [
            'lang',
            'publisher',
            'work',
            'work_isbn',
            'work_language',
            'work_weight',
            'author',
            'work_author',
            'subject',
            'work_subject',
        ]

        self.__output_files = None
        self.__language_file = None

        self.__publisher_id = itertools.count(1)
        self.__work_id = itertools.count(1)
        self.__author_id = itertools.count(1)

        self.__publishers = {}
        self.__author_ids = {}
        self.__work_authors = {}
        self.work_ids = {}
        
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.__create_db_schema()

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

        self.__language_file = open(rf'open library dump\data\lang.{self.type_name}', 'w', encoding='utf-8', newline='')

        with open(input_file, 'r', encoding='utf-8') as f_in:
            for line in f_in:
                obj = orjson.loads(line.split('\t')[4])

                for key, value in obj.items():
                    if isinstance(value, str):
                        obj[key] = value.replace('\n', ' ').replace('\r', ' ')

                if (row_title := self.parse_id(obj.get('type')['key'])) in self.__type_mapping:
                    try:
                        if func:=self.__type_mapping.get(row_title, None):
                            func(obj)
                    except Exception:
                        continue
                    
        return self.__output_files

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

        self.__output_files = {type_name: open(os.path.join(directory,
                            rf'data\{type_name}.{self.type_name}'), 'w', encoding='utf-8', newline='')
                                    for type_name in self.__normalized_types}
        self.__output_files = self.process_file(latest_file.path)

        self.user_manager.writePfp()
        self.__write_subjects()
        self.__write_dictionary(self.__publishers, 'publisher')
        self.__write_work_authors(self.__work_authors, 'work_author')

        for f_out in self.__output_files.values():
            f_out.close()
            
        self.conn.commit()
        self.cursor.close()
        return [self.user_manager.get_user_file(), self.user_manager.get_pfp_file()] + [rf'open library dump\data\{type_name}.{self.type_name}'
                for type_name in self.__normalized_types]

    def __process_edition(self, obj: dict) -> dict:
        """
        Process an edition object and return a dictionary of parsed data.

        Args:
            obj (dict): The edition object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        old_id = [self.parse_id(work['key']) for work in obj.get('works', [{"key": ""}])][0]
        work_id = self.__get_new_id(old_id, self.work_ids, self.__work_id)
        
        if not (title := self.__build_title(obj)) or not (created := self.__get_created(obj)):
            return

        isbns = obj.get('isbn_13', obj.get('isbn_10', []))
        isbns = self.convert_to_isbn13(isbns)

        self.cursor.execute('SELECT * FROM work_isbn WHERE work_id = ?', (work_id,))
        unique_flag = False
        if not self.cursor.fetchone() and isbns:
            self.__print_normalized(work_id, 'work_isbn', [isbns[0]])
            unique_flag = True
        self.cursor.executemany('INSERT OR IGNORE INTO work_isbn VALUES (?, ?)', [(work_id, isbn) for isbn in isbns])

        if not unique_flag:
            return

        number_of_pages = abs(obj.get('number_of_pages', 0))

        publisher = 'Other' if not (publishers := [self.__html_escape(p) for p in obj.get('publishers', [])]) else self._capitalize_first(publishers[0])
        if not (publisher_id := self.__publishers.get(publisher, None)):
            publisher_id = next(self.__publisher_id)
            self.__publishers[publisher] = publisher_id
        
        if languages := [self.parse_id(lang['key']) for lang in obj.get('languages', [])] :
            self.__print_normalized(work_id, 'work_language', [languages[0]])

        publish_date = obj.get('publish_date')
        year = self.__find_year(publish_date) if publish_date else 0
        year = random.randint(1900, 2022) if year == 0 else year

        if weight := obj.get('weight', None):
            if kg_weight := self.__find_weight_in_kg(weight):
                self.__print_normalized(work_id, 'work_weight', [kg_weight])

        self._write_strategy(self.__output_files['work'], {
            'work_id': work_id,
            'publisher_id': publisher_id,
            'title': title,
            'number_of_pages': number_of_pages,
            'release_year': year,
            'created': created,
        })

    def __process_work(self, obj: dict) -> dict:
        """
        Process a work object and return a dictionary of parsed data.

        Args:
            obj (dict): The work object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        old_id = self.parse_id(obj.get('key', None))
        work_id = self.__get_new_id(old_id, self.work_ids, self.__work_id)
       
        subjects = [self._capitalize_first(self.__html_escape(subject)) for subject in obj.get('subjects', [])]
        self.cursor.executemany('INSERT OR IGNORE INTO work_subject VALUES (?, ?)', [(work_id, subject) for subject in subjects])
    
        work_authors = [self.parse_id(author['author']['key']) for author in obj.get('authors', [])]
        author_ids = [ str(self.__get_new_id(old_id, self.__author_ids, self.__author_id)) for old_id in work_authors]
        
        for author_id in author_ids:
            self.__work_authors.setdefault(work_id, set()).add(author_id)
    
    def __process_author(self, obj: dict) -> dict:
        """
        Process an author object and return a dictionary of parsed data.

        Args:
            obj (dict): The author object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        if not (created := self.__get_created(obj)) or not (name := self.__html_escape(obj.get('name', ''))):
            return
        old_id = self.parse_id(obj.get('key', None))
        author_id = self.__get_new_id(old_id, self.__author_ids, self.__author_id)

        self._write_strategy(self.__output_files['author'], {
            'id': author_id,
            'name': name,
            'created': created
        })

    def __process_language(self, obj: dict) -> dict:
        """
        Process a language object and return a dictionary of parsed data.

        Args:
            obj (dict): The language object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        if not (created := self.__get_created(obj)):
            return
        id = self.parse_id(obj.get('key', None))
        name = obj.get('name', '')

        language = {
            'id': id,
            'name': name,
            'speakers': speakers.get(id, 0),
            'added_at': created,
        }

        self._write_strategy(self.__language_file, language)
        self.__language_file.flush()

    def __process_subject(self, obj: dict) -> None:
        name = obj.get("name")
        created = self.__get_created(obj)

        self.cursor.execute('INSERT OR IGNORE INTO subject(subject_name, created_at) VALUES (?, ?)', (name, created))

    @classmethod
    def __get_created(cls, obj: dict) -> str:
        """
        Get the created timestamp from an object.

        Args:
            obj (dict): The object from which to extract the created timestamp.

        Returns:
            str: The created timestamp.
        """
        return obj.get('created', {}).get('value', '') \
            if obj.get('created') else obj.get('last_modified', {}).get('value', '') \
            if obj.get('last_modified') else datetime.now().isoformat()

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
        s = html.unescape(s).rstrip('\\')
        return s.strip(punctuation+whitespace).replace('"', "'")

    def __print_normalized(self, id: str, name: str, obj: list) -> None:
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

            self._list_write_strategy(self.__output_files[name], [item for item in [{ "id": id, name : unit } for unit in obj if unit]])

    @classmethod
    def __find_year(cls, s: str) -> int:
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
    def __find_weight_in_kg(cls, s: str) -> float:
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

    def __write_dictionary(self, dictionary: dict, name: str) -> None:
        """
        Writes the given dictionary to the output file with the specified name.

        Args:
            dictionary (dict): The dictionary to be written.
            name (str): The name of the output file.

        Returns:
            None
        """
        self._dict_write_strategy(self.__output_files[name], dictionary)
        dictionary = {}

    def __write_work_authors(self, dictionary: dict[int, set], name: str) -> None:
        """
        Writes the given dictionary of sets to the output file with the specified name.

        Args:
            dictionary (dict[int, set]): The dictionary of sets to be written.
            name (str): The name of the output file.

        Returns:
            None
        """
        for key, values in dictionary.items():
            if key in self.work_ids:
                for value in values:
                    self._write_strategy(self.__output_files[name], { "work_id": key, "author_id" : value })        
        dictionary = {}

    def __build_title(self, obj: dict) -> str:
        """
        Build the title string based on the given object.

        Args:
            obj (dict): The object containing the title information.

        Returns:
            str: The built title string.
        """
        title_builder = io.StringIO()
        title_builder.write(obj.get('title_prefix', ''))
        title_builder.write('. ')
        title_builder.write(obj.get('title', ''))
        title_builder.write(': ')
        title_builder.write(obj.get('subtitle', ''))
        title_builder.write(' ')
        title_builder.write(obj.get('by_statement', ''))
        title = self.__html_escape(title_builder.getvalue())
        return title

    def __get_new_id(self, old_id: str, id_dict: dict, id_gen: itertools.count) -> str:
        if not (new_id:= id_dict.get(old_id, None)):
            new_id = next(id_gen)
            id_dict[old_id] = new_id
        return new_id

    def __write_subjects(self) -> None:
        """
        Writes the subjects to the output file.

        Returns:
            None
        """

        select_count10_or_max = """
            WITH frequent_subjects AS (
                SELECT
                    subject_name
                FROM
                    work_subject
                GROUP BY
                    subject_name
                HAVING
                    COUNT(*) >= 10
                    
            UNION

                SELECT
                    subject_name
                FROM
                    work_subject
                GROUP BY
                    work_id, subject_name
                HAVING
                    COUNT(*) = (
                        SELECT
                            MAX(cnt)
                        FROM (
                            SELECT
                                COUNT(*) as cnt
                            FROM
                                work_subject
                            GROUP BY
                                work_id, subject_name
                        )
                    )
            )
        """

        self.cursor.execute(select_count10_or_max + '''
            SELECT 
                work_subject.work_id,
                subject.subject_id
            FROM 
                work_subject
            JOIN 
                subject
            ON 
                work_subject.subject_name = subject.subject_name
            WHERE 
                work_subject.subject_name IN frequent_subjects
        ''')
        self._sqlite_write_strategy(self.__output_files['work_subject'], self.cursor.fetchall())

        self.cursor.execute(select_count10_or_max + '''
            SELECT 
                subject.subject_id,
                subject.subject_name
            FROM 
                subject
            JOIN 
                frequent_subjects
            ON 
                subject.subject_name = frequent_subjects.subject_name
        ''')

        self._sqlite_write_strategy(self.__output_files['subject'], self.cursor.fetchall())

    def __create_db_schema(self):
            """
            Creates the database schema for the Open Library dump parser.

            This method drops existing tables if they exist and creates new tables with the following structure:
            - work_isbn: work_id (TEXT), isbn (TEXT), primary key (work_id, isbn)
            - subject: subject_id (INTEGER), subject_name (TEXT), created_at (timestamp), primary key (subject_id)
            - work_subject: work_id (TEXT), subject_name (TEXT), primary key (work_id, subject_name)

            Indexes are also created on the work_isbn and work_subject tables.

            Returns:
                None
            """
            self.cursor.execute('''
                DROP TABLE IF EXISTS work_isbn;
            ''')
            self.cursor.execute('''
                DROP TABLE IF EXISTS work_subject;
            ''')
            self.cursor.execute('''
                DROP TABLE IF EXISTS subject;
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS work_isbn (
                    work_id TEXT,
                    isbn TEXT,
                    PRIMARY KEY(work_id, isbn)
                )
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_work_id ON work_isbn(work_id);
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_isbn ON work_isbn(isbn);
            ''')
            self.cursor.execute('''
                CREATE TABLE subject(
                    subject_id INTEGER PRIMARY KEY,
                    subject_name TEXT UNIQUE NOT NULL,
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS subject_idx_work_id ON subject(subject_name);
            ''')
            self.cursor.execute('''
                CREATE TABLE work_subject(
                    work_id TEXT,
                    subject_name TEXT NOT NULL,
                    PRIMARY KEY(work_id, subject_name)
                )
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS work_subject_idx_work_id ON work_subject(work_id);
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS work_subject_idx_isbn ON work_subject(subject_name);
            ''')