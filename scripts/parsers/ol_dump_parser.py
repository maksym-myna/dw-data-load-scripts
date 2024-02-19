from parsers.ol_abstract_parser import OLAbstractParser
from .abstract_parser import AbstractParser
from datetime import datetime
import orjson
import html
import os
import re


class OLDumpParser(OLAbstractParser):
    """
    A class for parsing Open Library dump files.

    Attributes:
        type_mapping (dict): Mapping type names to corresponding processing methods.
    """

    def process_file(self, input_file, output_files):
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
                type_name = self.parse_id(obj.get('type')['key'])
                if type_name in self.__type_mapping:
                    try:
                        json_obj = self.__type_mapping[type_name](self, obj)
                        output_files[type_name].write(
                            orjson.dumps(json_obj).decode('utf-8') + '\n')
                    except Exception:
                        continue
        return output_files

    def process_latest_file(self, directory):
        """
        Process the latest dump file in the given directory.

        Args:
            directory (str): The path to the directory containing dump files.

        Returns:
            None
        """
        pattern = pattern = re.compile(r'ol_dump_\d{4}-\d{2}-\d{2}.txt')

        # Get a list of all files in the directory that match the pattern
        files = (entry for entry in os.scandir(directory) if entry.is_file() \
            and pattern.match(entry.name))

        # Get the latest file
        latest_file = max(files, key=lambda f: f.name)

        if not AbstractParser.is_path_valid(directory):
            raise NotADirectoryError(directory)

        output_files = {type_name: open(os.path.join(directory, \
            rf'data\{type_name}.jsonl'), 'w', encoding='utf-8') \
                for type_name in self.__type_mapping}

        output_files = self.process_file(latest_file.path, output_files)

        for f_out in output_files.values():
            f_out.close()

        return [rf'open library dump\data\{type_name}.jsonl'\
            for type_name in self.__type_mapping]

    def __process_edition(self, obj):
        """
        Process an edition object and return a dictionary of parsed data.

        Args:
            obj (dict): The edition object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        title = obj.get('title', '')
        subtitle = obj.get('subtitle', '')
        title = self.__html_escape(f'{title}: {subtitle}') if subtitle else title
        publishers = [self.__html_escape(publisher)\
            for publisher in obj.get('publishers', [])]
        isbn_10 = obj.get('isbn_10', [])
        isbn_13 = obj.get('isbn_13', [])
        series = obj.get('series', [])
        languages = [self.parse_id(lang['key']) for lang in obj.get('languages', [])]
        number_of_pages = obj.get('number_of_pages', 0)
        publish_date = obj.get('publish_date', '')
        works = [self.parse_id(work['key']) for work in obj.get('works', [])]
        id = self.parse_id(obj.get('key', ''))

        return {
            'title': title,
            'publishers': publishers,
            'isbn_10': isbn_10,
            'isbn_13': isbn_13,
            'series': series,
            'languages': languages,
            'number_of_pages': number_of_pages,
            'created': created,
            'publish_date': publish_date,
            'works': works,
            'id': id
        }

    def __process_work(self, obj):
        """
        Process a work object and return a dictionary of parsed data.

        Args:
            obj (dict): The work object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        title = obj.get('title', '')
        subtitle = obj.get('subtitle', '')
        title = self.__html_escape(f'{title}: {subtitle}') if subtitle else title
        authors = [self.parse_id(author['author']['key'])\
            for author in obj.get('authors', [])]
        id = self.parse_id(obj.get('key', ''))
        subjects = [self.__html_escape(subject) for subject in obj.get('subjects', [])]
        return {
            'title': title,
            'authors': authors,
            'created': created,
            'id': id,
            'subjects': subjects
        }

    def __process_author(self, obj):
        """
        Process an author object and return a dictionary of parsed data.

        Args:
            obj (dict): The author object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        id = self.parse_id(obj.get('key', ''))
        name = self.__html_escape(obj.get('name', ''))
        return {
            'id': id,
            'created': created,
            'name': name
        }

    def __process_language(self, obj):
        """
        Process a language object and return a dictionary of parsed data.

        Args:
            obj (dict): The language object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        created = self.__get_created(obj)
        id = self.parse_id(obj.get('key', ''))
        name = obj.get('name', '')
        return {
            'id': id,
            'name': name,
            'created': created
        }

    @classmethod
    def __get_created(cls, obj):
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
    def __html_escape(cls, s):
        """
        Escapes HTML entities in a string.

        Args:
            s (str): The string to escape.

        Returns:
            str: The escaped string.
        """
        return html.unescape(s)

    __type_mapping = {
        'edition': __process_edition,
        'work': __process_work,
        'author': __process_author,
        'language': __process_language
    }
