import csv
import io
import os
import re
import random
import sqlite3
import itertools
from math import ceil
from datetime import datetime
from string import punctuation, whitespace

import numpy as np
from orjson import loads as jsonloads
from html import unescape
import pandas as pd
from transliterate import translit
from nltk import pos_tag, download
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer
from nltk.tokenize import word_tokenize
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from lingua import LanguageDetectorBuilder

from parsers.ol_abstract_parser import OLAbstractParser
from parsers.file_writer import FileWriter
from parsers.user_manager import UserManager
from .abstract_parser import AbstractParser
from language_speakers import speakers

download("wordnet", quiet=True)
download("punkt", quiet=True)
download("averaged_perceptron_tagger", quiet=True)
download("stopwords", quiet=True)


class OLDumpParser(OLAbstractParser, FileWriter):
    """
    A class for parsing Open Library dump files.

    Attributes:
        type_mapping (dict): Mapping type names to corresponding processing methods.
    """

    punctuation_without_braces = punctuation.translate(str.maketrans('', '', '(){}[]'))

    def __init__(
        self, conn: sqlite3.Connection, file_type: str, user_manager: UserManager
    ) -> None:
        """
        Initializes an OLDumpParser object.

        Args:
            conn (sqlite3.Connection): The SQLite database connection.
            file_type (str): The type of file being parsed.
            user_manager (UserManager): The user manager object.

        Params:
            __type_mapping (dict): Mapping type names to corresponding processing methods.
            __normalized_types (list[str]): A list of normalized type names.
            __language_detector (LanguageDetector): A language detector object.
            __lemmatizer (WordNetLemmatizer): A WordNet lemmatizer object.
            __stop_words (set): A set of stop words.
            __output_files (dict): A dictionary of output file objects.
            __language_file (TextIOWrapper): The language file object.
            __work_id (itertools.count): An iterator that generates work IDs.
            __author_id (itertools.count): An iterator that generates author IDs.
            __author_ids (dict): A dictionary mapping old author IDs to new author IDs.
            __work_authors (dict): A dictionary mapping work IDs to author IDs.
            work_ids (dict): A dictionary mapping old work IDs to new work IDs.
        Returns:
            None
        """
        OLAbstractParser.__init__(self, user_manager, conn)
        FileWriter.__init__(self, file_type)

        self.__type_mapping = {
            "edition": self.__process_edition,
            "work": self.__process_work,
            "author": self.__process_author,
            "language": self.__process_language,
            "subject": self.__process_subject,
        }

        self.__normalized_types = [
            "lang",
            "publisher",
            "work",
            "author",
            "work_author",
            "subject",
            "work_subject",
        ]

        self.__language_mapping = {
            "bel": None,
            "rus": None,
            "fre": "fra",
            "cze": "ces",
            "wel": "cym",
            "ger": "deu",
            "gre": "ell",
            "baq": "eus",
            "per": "fas",
            "chi": "zho",
            "ice": "isl",
            "arm": "hye",
            "mac": "mkd",
            "dut": "nld",
            "slo": "slk",
            "geo": "kat",
            "rum": "ron",
            "may": "msa",
            "alb": "sqi",
            "mao": "mri"
        }

        self.__language_detector = LanguageDetectorBuilder.from_all_languages().build()

        self.__lemmatizer = WordNetLemmatizer()
        self.__stop_words = set(stopwords.words("english") + ["et", "de"])

        self.__output_files = None
        self.__language_file = None

        self.__work_id = itertools.count(1)
        self.__publisher_id = itertools.count(1)
        self.__author_id = itertools.count(1)

        self.__author_ids = {}
        self.__work_authors = {}
        self.__publishers = {}
        self.work_ids = {}

        self.cursor.executescript(
            open("scripts/sql/sqlite_schema.sql", "r", encoding="utf-8").read()
        )


    def process_file(self, input_file: str, output_file: str | None = None) -> list[str]:
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

        self.__language_file = open(
            rf"open library dump\data\lang.{self.type_name}",
            "w",
            encoding="utf-8",
            newline="",
        )

        with open(input_file, "r", encoding="utf-8") as f_in:
            for line in f_in:
                obj = jsonloads(line.split("\t")[4])

                for key, value in obj.items():
                    if isinstance(value, str):
                        obj[key] = value.replace("\n", " ").replace("\r", " ")
                if (row_title := self.parse_id(obj.get("type")["key"])) in self.__type_mapping:
                    try:
                        if func := self.__type_mapping.get(row_title, None):
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
        pattern = pattern = re.compile(r"ol_dump_\d{4}-\d{2}-\d{2}.txt")

        files = (
            entry
            for entry in os.scandir(directory)
            if entry.is_file() and pattern.match(entry.name)
        )

        latest_file = max(files, key=lambda f: f.name)

        if not AbstractParser.is_path_valid(directory):
            raise NotADirectoryError(directory)

        os.makedirs(rf"{directory}\data", exist_ok=True)
        self.__output_files = {
            type_name: open(
                os.path.join(directory, rf"data\{type_name}.{self.type_name}"),
                "w",
                encoding="utf-8",
                newline="",
            )
            for type_name in self.__normalized_types
        }

        self.__output_files = self.process_file(latest_file.path)

        self.user_manager.writePfp()
        self.__write_publishers()
        self.__write_subjects()
        self.__write_authors()

        for f_out in self.__output_files.values():
            f_out.close()

        self.conn.commit()
        self.cursor.close()
        return [
            self.user_manager.get_user_file(),
            self.user_manager.get_pfp_file(),
        ] + [
            rf"open library dump\data\{type_name}.{self.type_name}"
            for type_name in self.__normalized_types
        ]

    def __get_edition_work_id(self, obj: dict) -> int:
        """
        Retrieves the edition work ID from the given object.

        Args:
            obj (dict): The object containing the edition work information.

        Returns:
            int: The edition work ID.

        """
        old_id = [
            self.parse_id(work["key"]) for work in obj.get("works", [{"key": ""}])
        ][0]
        return self.__get_new_id(old_id, self.work_ids, self.__work_id)

    def __get_language(self, obj: dict, title: str) -> str:
        """
        Get the language of the given object based on the available languages and the title.

        Args:
            obj (dict): The object containing the languages.
            title (str): The title of the object.

        Returns:
            str: The language of the object, or None if the language is not available or is "bel" or "rus".
        """
        languages = [self.parse_id(lang["key"]) for lang in obj.get("languages", [])]
        language = None
        if not languages:
            if language := self.__language_detector.detect_language_of(title):
                language = language.iso_code_639_3.name.lower()
        else:
            language = languages[0]

        return self.__map_language(language)

    def __get_isbn(self, obj: dict, work_id: int) -> str:
        """
        Retrieves the ISBN for a given object and work ID.

        Args:
            obj (dict): The object containing the ISBN information.
            work_id (int): The ID of the work.

        Returns:
            str: The ISBN of the work.

        """
        isbns = obj.get("isbn_13", obj.get("isbn_10", []))
        isbns = self.convert_to_isbn13(isbns)

        if (
            not isbns
            or self.cursor.execute(
                "SELECT * FROM work_isbn WHERE work_id = ? LIMIT 1", (work_id,)
            ).fetchone()
        ):
            return

        self.cursor.executemany(
            "INSERT OR IGNORE INTO work_isbn VALUES (?, ?)",
            [(work_id, isbn) for isbn in isbns],
        )

        return isbns[0]

    def __get_publisher_id(self, obj: dict, ukrainian_flag: bool = False) -> int:
        """
        Get the publisher ID for a given object.

        Args:
            obj (dict): The object containing the publisher information.
            ukrainian_flag (bool, optional): Flag indicating whether to transliterate the publisher name to Ukrainian. Defaults to False.

        Returns:
            int: The publisher ID.

        """
        publisher = self.capitalize_first(
            self.__html_escape(obj.get("publishers", ["Unknown"])[0])
        )
        if ukrainian_flag and publisher != "Unknown":
            publisher = self.transliterate_to_ukrainian(publisher, publisher=True)

        if not (publisher_id := self.__publishers.get(publisher)) and (
            publisher_id := next(self.__publisher_id)
        ):
            self.__publishers[publisher] = publisher_id

        return publisher_id

    def __process_edition(self, obj: dict) -> dict:
        """
        Process an edition object and return a dictionary of parsed data.

        Args:
            obj (dict): The edition object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        work_id = self.__get_edition_work_id(obj)

        if (
            not (title := self.__build_title(obj))
            or not (language := self.__get_language(obj, title))
            or not (isbn := self.__get_isbn(obj, work_id))
        ):
            return

        ukrainian_flag = language == "ukr"
        publisher_id = self.__get_publisher_id(obj, ukrainian_flag)
        if ukrainian_flag:
            title = self.transliterate_to_ukrainian(title)

        created = self.__get_created(obj)
        number_of_pages = abs(obj.get("number_of_pages", 0))

        if published_at := obj.get("publish_date"):
            published_at = self.__find_year(published_at)
        if not published_at:
            published_at = random.randint(1900, 2022)

        if weight := obj.get("weight"):
            weight = self.__find_weight_in_kg(weight)
        if not weight:
            weight = self.__calculate_weight(number_of_pages)

        self._write_strategy(
            self.__output_files["work"],
            {
                "work_id": work_id,
                "publisher_id": publisher_id,
                "isbn": isbn,
                "language_id": language,
                "title": title,
                "number_of_pages": number_of_pages,
                "weight": weight,
                "release_year": published_at,
                "created": created,
            },
        )

    def __insert_subjects(self, obj: dict, work_id: int) -> None:
        """
        Inserts subjects into the work_subject table.

        Args:
            obj (dict): The object containing the subjects.
            work_id (int): The ID of the work.

        Returns:
            None
        """
        subjects = [
            self.capitalize_first(self.__html_escape(subject))
            for subject in obj.get("subjects", [])
        ]
        self.cursor.executemany(
            "INSERT OR IGNORE INTO work_subject VALUES (?, ?)",
            [(work_id, subject) for subject in subjects],
        )

    def __insert_authors(self, obj: dict, work_id: int) -> None:
        """
        Inserts authors into the work_authors dictionary.

        Args:
            obj (dict): The object containing author information.
            work_id (int): The ID of the work.

        Returns:
            None
        """
        work_authors = [
            self.parse_id(author["author"]["key"]) for author in obj.get("authors", [])
        ]
        author_ids = [
            str(self.__get_new_id(old_id, self.__author_ids, self.__author_id))
            for old_id in work_authors
        ]

        for author_id in author_ids:
            self.__work_authors.setdefault(work_id, set()).add(author_id)

    def __process_work(self, obj: dict) -> dict:
        """
        Process a work object and return a dictionary of parsed data.

        Args:
            obj (dict): The work object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        old_id = self.parse_id(obj.get("key", None))
        work_id = self.__get_new_id(old_id, self.work_ids, self.__work_id)

        self.__insert_subjects(obj, work_id)
        self.__insert_authors(obj, work_id)

    def __process_author(self, obj: dict) -> dict:
        """
        Process an author object and return a dictionary of parsed data.

        Args:
            obj (dict): The author object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        if not (name := self.__html_escape(obj.get("name", ""))):
            return
        created = self.__get_created(obj)

        old_id = self.parse_id(obj.get("key", None))
        author_id = self.__get_new_id(old_id, self.__author_ids, self.__author_id)

        self.cursor.execute("INSERT OR IGNORE INTO author_id VALUES (?)", (author_id,))

        self._write_strategy(
            self.__output_files["author"],
            {"id": author_id, "name": name, "created": created},
        )

    def __process_language(self, obj: dict) -> dict:
        """
        Process a language object and return a dictionary of parsed data.

        Args:
            obj (dict): The language object to be processed.

        Returns:
            dict: A dictionary containing the parsed data.
        """
        if not (id := self.parse_id(obj.get("key"))):
            return

        id = self.__map_language(id)

        name = obj.get("name", "")
        created = self.__get_created(obj)

        language = {
            "id": id,
            "name": name,
            "speakers": speakers.get(id, 0),
            "added_at": created,
        }

        self._write_strategy(self.__language_file, language)
        self.__language_file.flush()

    def __process_subject(self, obj: dict) -> None:
        """
        Process the subject object and insert it into the subject table.

        Args:
            obj (dict): The subject object to be processed.

        Returns:
            None
        """
        name = obj.get("name")
        created = self.__get_created(obj)

        self.cursor.execute(
            "INSERT OR IGNORE INTO subject(subject_name, created_at) VALUES (?, ?)",
            (name, created),
        )

    @staticmethod
    def __get_created(obj: dict) -> str:
        """
        Get the created timestamp from an object.

        Args:
            obj (dict): The object from which to extract the created timestamp.

        Returns:
            str: The created timestamp.
        """
        return (
            obj.get("created", {}).get("value", "")
            if obj.get("created")
            else (
                obj.get("last_modified", {}).get("value", "")
                if obj.get("last_modified")
                else datetime.now().isoformat()
            )
        )

    @staticmethod
    def __html_escape(s: str) -> str:
        """
        Escapes HTML entities in a string.

        Args:
            s (str): The string to escape.

        Returns:
            str: The escaped string.
        """
        s = (
            s.replace("&NewLine", " ")
            .rstrip("&#13")
            .rstrip("&#10")
            .replace("&#10;", "")
            .replace("&#13;", "")
            .replace('"', "")
            .replace("&quot;", "")
        )
        s = unescape(s).rstrip("\\")
        return s.strip(OLDumpParser.punctuation_without_braces + whitespace).replace('"', "'")

    def __build_title(self, obj: dict) -> str:
        """
        Build the title string based on the given object.

        Args:
            obj (dict): The object containing the title information.

        Returns:
            str: The built title string.
        """
        title_builder = io.StringIO()
        title_builder.write(obj.get("title_prefix", ""))
        title_builder.write(". ")
        title_builder.write(obj.get("title", ""))
        title_builder.write(": ")
        title_builder.write(obj.get("subtitle", ""))
        title_builder.write(" ")
        title_builder.write(obj.get("by_statement", ""))
        title = self.__html_escape(title_builder.getvalue())
        title = self.process_name(title, capitalize_first=True)
        return title

    def __get_new_id(self, old_id: str, id_dict: dict, id_gen: itertools.count) -> str:
        """
        Returns a new ID for the given old ID, using the provided ID dictionary and ID generator.

        Args:
            old_id (str): The old ID for which a new ID is needed.
            id_dict (dict): A dictionary mapping old IDs to new IDs.
            id_gen (itertools.count): An iterator that generates new IDs.

        Returns:
            str: The new ID corresponding to the old ID.

        Raises:
            StopIteration: If the ID generator is exhausted and no new ID can be generated.

        """
        try:
            if not (new_id := id_dict.get(old_id, None)):
                new_id = next(id_gen)
                id_dict[old_id] = new_id
        except StopIteration:
            new_id = None
        return new_id

    @staticmethod
    def __find_year(s: str) -> int:
        """
        Finds the first occurrence of a 4-digit year in a given string.

        Args:
            s (str): The input string to search for a year.

        Returns:
            int: The found year if it is less than or equal to the current year, otherwise 0.
        """
        current_year = datetime.now().year
        pattern = re.compile(r"\b\d{4}\b")
        for match in pattern.finditer(s):
            year = int(match.group())
            if year <= current_year:
                return year
        return 0

    @staticmethod
    def __find_weight_in_kg(s: str) -> float:
        """
        Finds the weight in kilograms from a given string.

        Args:
            s (str): The input string.

        Returns:
            float: The weight in kilograms, rounded to 2 decimal places.
                    Returns None if the weight cannot be found.
        """
        pattern = re.compile(r"\d+(\.\d+)?")
        result = 0
        if "k" in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) if result else None
        elif "g" in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 1000 if result else None
        elif "z" in s or "ounc" in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 35.284 if result else None
        elif "lb" in s or "pound" in s:
            result = list(pattern.finditer(s))
            result = float(result[0].group()) / 2.205 if result else None
        return round(result, 2) if result else None

    def __calculate_weight(self, pages: int, page_weight: float = 0.0025) -> float:
        """
        Calculate the weight of a book based on the number of pages.

        Args:
            pages (int): The number of pages in the book.

        Returns:
            float: The calculated weight in kilograms.
        """
        return round(pages * page_weight, 2) + 20

    @staticmethod
    def __process_author_row(row, authors_with_works: set[str], authors: dict[str, tuple[int,str,datetime]], author_names_collisions: dict[str,int]) -> None:
        if row["author_id"] not in authors_with_works:
            return
        if row["full_name"] not in authors:
            authors[row["full_name"]] = (row["author_id"], row["full_name"], row["added_at"])
        else:
            author_names_collisions[row["author_id"]] = authors[row["full_name"]][0]


    @staticmethod
    def __chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def __write_authors(self) -> None:
        """
        Writes the given dictionary of sets to the output file with the specified name.

        Args:
            dictionary (dict[int, set]): The dictionary of sets to be written.
            name (str): The name of the output file.

        Returns:
            None
        """
        AUTHOR_LOCATION = rf"open library dump\data\author.{self.type_name}"
        WORK_AUTHOR_LOCATION = rf"open library dump\data\work_author.{self.type_name}"
        WORK_LOCATION = rf"open library dump\data\work.{self.type_name}"
        CHUNK_SIZE = 1000

        work_authors = []
        work_ids = self.cursor("SELECT work_id FROM work_id").fetchall()
        keys = list(self.__work_authors.keys())

        for chunk in self.__chunks(keys, CHUNK_SIZE):
            for work_id in chunk:
                if work_id in work_ids and (
                    author_ids := self.__work_authors.get(work_id, None)
                ):
                    for author_id in author_ids:
                        self.cursor.execute(
                            "SELECT 1 FROM author_id WHERE author_id = ? LIMIT 1",
                            (author_id,),
                        )
                        if self.cursor.fetchone():
                            work_authors.append((work_id, author_id))
        del self.__work_authors

        authors = {}
        author_names_collisions = {}
        authors_with_works = {author for work, author in work_authors}

        with open(AUTHOR_LOCATION, "r", encoding="utf-8") as file:
            if self.type_name == 'csv':
                reader = csv.DictReader(file, fieldnames=["author_id", "full_name", "added_at"])
                for row in reader:
                    self.__process_author_row(row, authors_with_works, authors, author_names_collisions)
            elif self.type_name == 'jsonl':
                for line in file:
                    row = jsonloads(line)
                    self.__process_author_row(row, authors_with_works, authors, author_names_collisions)

        with open(AUTHOR_LOCATION, "w", encoding="utf-8", newline="") as file:
            self._tuple_write_strategy(file, list(authors.values()))

        with open(WORK_AUTHOR_LOCATION, "w", encoding="utf-8", newline="") as file:
            self._tuple_write_strategy(file, {(work_id, author_names_collisions.get(author_id, author_id)) for work_id, author_id in work_authors})


    @staticmethod
    def process_name(s: str, title: bool = False, capitalize_first: bool = False) -> str:
        stack = []
        i = 0
        while i < len(s):
            if s[i] in '([{':
                stack.append((s[i], i))
            elif s[i] in ')]}':
                if stack and '([{'.index(stack[-1][0]) == ')]}'.index(s[i]):
                    stack.pop()
                else:
                    s = s[:i] + s[i+1:]
                    continue  # Skip the increment of i because the string has been shortened
            i += 1

        while stack:
            _, i = stack.pop()
            s = s[:i]

        s = s.rstrip(whitespace + OLDumpParser.punctuation_without_braces)
        s = re.sub(' +', ' ', s)

        if title:
            s = s.title()
        elif capitalize_first:
            s = OLDumpParser.capitalize_first(s)

        return s

    def __write_publishers(self) -> None:
        """
        Writes the publishers to the output file.

        Returns:
            None
        """
        if self.type_name == 'jsonl':
            self._write_strategy(self.__output_files["publisher"], self.__publishers)
            return

        WORK_LOCATION = rf"open library dump\data\work.{self.type_name}"
        NEW_WORK_LOCATION = rf"open library dump\data\new_work.{self.type_name}"
        PUBLISHER_LOCATION = rf"open library dump\data\publisher.{self.type_name}"
        SHORTENED_STRING_MAX_LENGTH = 50

        with open(PUBLISHER_LOCATION, "w", encoding="utf-8", newline="") as file:
            publisher_names = {
                name: self.preprocess_publisher(self.shorten_string(name, SHORTENED_STRING_MAX_LENGTH))
                for name in self.__publishers.keys()
            }

            preprocessed_names = [(name,) for name in publisher_names.values()]

            self.cursor.executemany(
                "INSERT OR IGNORE INTO processed_publisher(processed_publisher_name) VALUES (?)",
                preprocessed_names,
            )
            preprocessed_publishers = {
                pp_name: pp_id
                for pp_id, pp_name in self.cursor.execute(
                    "SELECT processed_publisher_id, processed_publisher_name FROM processed_publisher"
                ).fetchall()
            }

            publishers = [
                    (
                        self.__publishers[name],
                        name,
                        preprocessed_publishers[pp_name],
                    )
                    for name, pp_name in publisher_names.items()
                ]
            self.cursor.executemany(
                "INSERT OR IGNORE INTO publisher VALUES (?, ?, ?)",
                publishers,
            )

            publishers_tuple = self.cursor.execute(
                """
                WITH min_length AS (
                    SELECT
                        processed_publisher_id,
                        MIN(LENGTH(publisher_name)) AS min_length
                    FROM
                        publisher
                    GROUP BY
                        processed_publisher_id
                )
                SELECT
                    p.processed_publisher_id, p.publisher_name
                FROM
                    publisher p
                JOIN
                    min_length m
                ON
                    p.processed_publisher_id = m.processed_publisher_id
                AND
                    LENGTH(p.publisher_name) = m.min_length;
                """
                ).fetchall()

            new_publishers = {}
            publisher_collisions = {}
            for id, name in publishers_tuple:
                short_name = self.process_name(self.shorten_string(name, SHORTENED_STRING_MAX_LENGTH), title=True) or 'Unknown'
                if new_publishers.get(short_name):
                    publisher_collisions[id] = new_publishers[short_name]
                else:
                    new_publishers[short_name] = id

            unknown_id = max(map(int, new_publishers.values())) + 1
            if not new_publishers.get('Unknown'):
                new_publishers['Unknown'] = unknown_id
            else:
                unknown_id = new_publishers.get('Unknown')

            new_publishers = {v: k for k, v in new_publishers.items()}

            old_to_new_ids = {
                pid: ppid
                for pid, ppid in self.cursor.execute(
                    "SELECT publisher_id, processed_publisher_id FROM publisher order by publisher_id"
                ).fetchall()
            }

            old_to_new_ids.update(publisher_collisions)

            for key, value in old_to_new_ids.items():
                if not new_publishers.get(value):
                    old_to_new_ids[key] = unknown_id

            new_new_ids = {}

            keys = list(new_publishers.keys())
            new_publishers_updated = {}
            for i, id in enumerate(keys, start=1):
                new_new_ids[id] = i
                new_publishers_updated[i] = new_publishers[id]
            new_publishers = new_publishers_updated

            for key, value in old_to_new_ids.items():
                old_to_new_ids[key] = new_new_ids[value]

            publisher_ids = set()
            for chunk in pd.read_csv(
                WORK_LOCATION,
                names=[
                    "work_id",
                    "publisher_id",
                    "isbn",
                    "language_id",
                    "title",
                    "number_of_pages",
                    "weight",
                    "release_year",
                    "created",
                ],
                dtype={"isbn": str},
                chunksize=1_000_000,
            ):
                chunk = chunk.dropna(subset=["title"])
                chunk["publisher_id"] = chunk["publisher_id"].apply(
                    lambda old_id: old_to_new_ids[old_id]
                )
                publisher_ids.update(chunk["publisher_id"].dropna().values)
                chunk.to_csv(
                    NEW_WORK_LOCATION,
                    mode="a",
                    index=False,
                    header=False,
                    encoding="utf-8",
                    quoting=csv.QUOTE_ALL,
                )
                work_ids = chunk['work_id'].apply(lambda x: (x,)).tolist()
                self.cursor.executemany("INSERT OR IGNORE INTO work_id VALUES (?)", work_ids)
            self._tuple_write_strategy(
                file, [(pid, new_publishers[pid]) for pid in publisher_ids]
            )

        os.remove(WORK_LOCATION)
        os.rename(NEW_WORK_LOCATION, WORK_LOCATION)

    def preprocess_publisher(self, name: str) -> str:
        name = name.lower()
        s = re.sub(
            r"\b(імені|имени|ім|им|університет|інститут|національний|національна|нац|державний|державна|у|і|й|college|institute|textbook|textbooks|books|group|incorporated|inc|house|illustrate|australia|united|states|usa|us|uk|canada|india|publications|pubns|publisher|publishers|publishing|pub|primary|secondary|collection|collections|communication|communications|company|companies|co|imprints|imprint|reprints|reprint|imprints|prints|print|press|ltd|juvenile|juv|professional|pro|pr|school|educational|education|division|library|interest|international|int|intrnl|intl|intnl|literature|txt|general|management|hall|academy|academies|academic|learning|paperbacks|paperback|bros|home|audio|video|multimedia|media|story|stories|writer|writers|org|organization|org|panamerican|american|america|departments|department|archives|archive|editions|edition|editorials|editorial|biblioteked|biblioteka|biblioteke|bbc|junior|with|for|llc|vydvo|vyd-vo|edu|gp|news|university|univ|uni|t-vo|tvo|tovarystvo|government|govt|gov|the|in|of|at|an|a|de|et)\b",
            "",
            name,
        )
        try:
            if not s:
                words = name.split()
                articles = {"the", "a", "an", "de", "et", "in", "of", "at", "у", "і", "й"}
                name = next((word for word in words if word.lower() not in articles), words[0] if words else "Unknown")
        except Exception as e:
            print(name)
            print(s)
        s = re.sub(r"\W+", "", s, flags=re.UNICODE)
        return s.strip()

    def __write_subjects(self) -> None:
        """
        Writes the subjects to the output file.

        Returns:
            None
        """
        subjects = self.cursor.execute(
            "select subject_id, subject_name from subject"
        ).fetchall()

        processed_subjects = [self.preprocess(name) for id, name in subjects]

        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(processed_subjects)
        word_freq = np.sum(X.toarray(), axis=0)
        words = vectorizer.get_feature_names_out()

        n_general_subjects = 666
        general_subjects_idx = np.argpartition(word_freq, -n_general_subjects)[
            -n_general_subjects:
        ]
        general_subjects = [words[i] for i in general_subjects_idx]

        np_new_subjects, np_old_to_new_ids = self.generalize_subjects(
            subjects, general_subjects
        )

        combined = (
            list(np_new_subjects.values())
            + processed_subjects
            + list(np_new_subjects.values())
            + general_subjects
            + list(np_new_subjects.values())
        )

        vectorizer = TfidfVectorizer()
        vectors = vectorizer.fit_transform(combined)

        kmeans = MiniBatchKMeans(
            n_clusters=50,
            batch_size=ceil(len(combined) / 2),
            init="k-means++",
            n_init=10,
            max_iter=300,
        )
        kmeans.fit(vectors)

        new_subjects = {}
        old_to_new_ids = {}

        for i, (old_id, name) in enumerate(subjects):
            new_id = (kmeans.labels_[i] + 1).item()
            new_subjects[new_id] = name
            old_to_new_ids[old_id] = new_id

        for subject in subjects:
            old_id = subject[0]
            if not np_old_to_new_ids.get(old_id):
                new_id = old_to_new_ids[old_id]
                np_old_to_new_ids[old_id] = new_id
                np_new_subjects[new_id] = new_subjects[new_id]

        self.cursor.execute("DELETE FROM clustered_subject")
        self.cursor.execute("DELETE FROM subject_to_clustered_subject")
        self.cursor.executemany(
            "INSERT OR IGNORE INTO clustered_subject VALUES (?, ?)",
            [
                (key, self.process_name(value, capitalize_first=True))
                for key, value in np_new_subjects.items()
            ],
        )
        self.cursor.executemany(
            "INSERT OR IGNORE INTO subject_to_clustered_subject VALUES (?, ?)",
            [(old_id, new_id) for old_id, new_id in np_old_to_new_ids.items()],
        )

        self._tuple_write_strategy(
            self.__output_files["subject"],
            self.cursor.execute("SELECT * FROM clustered_subject").fetchall(),
        )

        del new_subjects
        del old_to_new_ids
        del subjects

        self.cursor.execute(
        """
            SELECT DISTINCT
                work_subject.work_id,
                clustered_subject.clustered_subject_id
            FROM
                work_subject
            JOIN
                subject
            ON
                work_subject.subject_name = subject.subject_name
            JOIN
                subject_to_clustered_subject
            ON
                subject.subject_id = subject_to_clustered_subject.subject_id
            JOIN
                clustered_subject
            ON
                clustered_subject.clustered_subject_id = subject_to_clustered_subject.clustered_subject_id
            JOIN
                work_id
            ON
                work_subject.work_id = work_id.work_id
            WHERE
                (clustered_subject.subject_name != 'Non-fiction')
                OR
                (clustered_subject.subject_name = 'Non-Fiction' AND work_subject.work_id NOT IN (
                    SELECT
                        work_id
                    FROM
                        work_subject ws
                    JOIN
                        subject s
                    ON
                        ws.subject_name = s.subject_name
                    JOIN
                        subject_to_clustered_subject scs
                    ON
                        s.subject_id = scs.subject_id
                    JOIN
                        clustered_subject cs
                    ON
                        cs.clustered_subject_id = scs.clustered_subject_id
                    WHERE
                        cs.subject_name != 'Non-Fiction'
                )
            )
        """
        )

        self._tuple_write_strategy(
            self.__output_files["work_subject"], self.cursor.fetchall()
        )
    @staticmethod
    def transliterate_to_ukrainian(text: str, publisher=False) -> str:
        """
        Transliterates the given text from a Latin-based script to Ukrainian Cyrillic script.

        Args:
            text (str): The text to be transliterated.
            publisher (bool, optional): Whether the text is a publisher name. Defaults to False.

        Returns:
            str: The transliterated text.

        """
        mapping = {
            "SHCH": "Щ",
            "shch": "щ",
            "yï": "иї",
            "i͡a︡": "я",
            "i︠a︡": "я",
            "i͡a": "я",
            "ia︡": "я",
            "íà": "я",
            "i͡u︡": "ю",
            "i︠u︡": "ю",
            "i͡u": "ю",
            "i͡e︡": "є",
            "i︠e︡": "є",
            "i͡e": "є",
            "i︠e": "є",
            "ĭ": "й",
            "ĭ": "й",
            "i︠︡": "i",
            "z͡h︡": "ж",
            "z︠h︡": "ж",
            "z͡h": "ж",
            "t͡s︡": "ц",
            "t︠s︡": "ц",
            "t͡s": "ц",
            "t︠s︠": "ц",
            "I͡A︡": "Я",
            "І︠А︡": "Я",
            "I︠A︡": "Я",
            "I͡A": "Я",
            "І︠а︡": "Я",
            "I︠a︡": "Я",
            "І︠У︡": "Ю",
            "I͡U︡": "Ю",
            "I︠U︡": "Ю",
            "I͡U": "Ю",
            "I͡u": "Ю",
            "I͡E︡": "Є",
            "І︠Е︡": "Є",
            "I͡E": "Є",
            "Z͡H︡": "Ж",
            "Z︠H︡": "Ж",
            "T͡S︡": "Ц",
            "T︠S︡": "Ц",
            "T͡S": "Ц",
            "/︠ ": "/ ",
            "--": "–",
            "ʹ'": "ь",
            "ʹ": "ь",
            "'": "ь",
            "w": "в",
            "W": "В",
            "ł": "в",
            "Ł": "В",
            "č": "ч",
            "Č": "Ч",
            "ǹ": "ьн",
            "n̆": "ьн",
            "š": "ш",
            "ö": "е",
            "i͏̈": "ї",
            "ia": "я",
            "ie": "є",
            "iu": "ю",
            "IA": "Я",
            "IE": "Є",
            "IU": "Ю",
            " ︡S": " С",
            "yi": "ий",
            "T́s̀": "Ц",
            "t́s̀": "ц",
            "źh̀": "ж",
        }
        for original, replacement in mapping.items():
            text = text.replace(original, replacement)
        text = translit(text, "uk")
        if publisher:
            text = (
                text.replace("Вид-во", "")
                .replace("Ізд-во", "")
                .replace("Видавництво", "")
                .replace("Вид.", "")
                .replace("Ін-т", "Інститут")
                .replace("ін-т", "інститут")
            )
        text = re.sub(r"[^\w\s]", "", text.strip(OLDumpParser.punctuation_without_braces + whitespace))
        text = re.sub(r"([бпвмфгкхжчшрБПВМФГКХЖЧШР])ь", r"\1", text)
        text = re.sub(r"([бпвмфгкхжчшр])([яюєї])", r"\1\'\2", text)

        return text

    def preprocess(self, text: str) -> str:
        """
        Preprocesses the given text by performing the following steps:
        1. Lowercases the text and removes punctuation.
        2. Tokenizes the text.
        3. Gets the part of speech tags.
        4. Removes words with specified parts of speech.
        5. Lemmatizes the words and removes stopwords.

        Args:
            text (str): The text to be preprocessed.

        Returns:
            str: The preprocessed text.

        """
        text = text.lower()
        text = re.sub(r'[^\w\s]', '', text)

        words = word_tokenize(text)

        tagged = pos_tag(words)

        remove_pos = ["JJ", "JJR", "JJS", "RB", "RBR", "RBS", "IN"]

        words = [word for word, pos in tagged if pos not in remove_pos]

        words = [
            self.__lemmatizer.lemmatize(word)
            for word in words
            if word not in self.__stop_words
        ]
        return " ".join(words)

    @staticmethod
    def generalize_subjects(
        subjects: list[tuple], general_subjects: list[str]
    ) -> tuple:
        """
        Generalizes the subjects based on a list of general subjects.

        Args:
            subjects (list): A list of tuples containing the old subject IDs and names.
            general_subjects (list): A list of general subjects to match against.

        Returns:
            tuple: A tuple containing two dictionaries:
                - new_subjects: A dictionary mapping new subject IDs to their corresponding general subjects.
                - old_to_new_ids: A dictionary mapping old subject IDs to their corresponding new subject IDs.
        """
        new_subjects = {}
        old_to_new_ids = {}
        new_id = 1
        other_id = None
        for i, (old_id, name) in enumerate(subjects):
            for gen_subj in general_subjects:
                if gen_subj in name.lower():
                    if gen_subj not in new_subjects.values():
                        new_subjects[new_id] = gen_subj
                        old_to_new_ids[old_id] = new_id
                        new_id += 1
                    break
            else:
                if not other_id:
                    new_subjects[new_id] = "Non-fiction"
                    other_id = new_id
                    new_id += 1
                old_to_new_ids[old_id] = new_id
        return new_subjects, old_to_new_ids

    def __map_language(self, language_id: str) -> str:
        """
        Maps the given language ID to a standardized language ID.

        Args:
            language_id (str): The language ID to be mapped.

        Returns:
            str: The mapped language ID.
        """
        return self.__language_mapping.get(language_id, language_id)

    @staticmethod
    def shorten_string(s: str, max_length: str):
        """
        Shortens a given string to a specified maximum length by removing stop words and stemming the words.

        Args:
            s (str): The input string to be shortened.
            max_length (int): The maximum length of the shortened string.

        Returns:
            str: The shortened string.

        """
        s = s.lstrip("Published by")
        s = s.split('by', 1)[0]
        if len(s) <= max_length:
            return s

        match = re.search(r'\\u\+\d{4}', s[:60])
        s = s[:match.start()] if match and match.end() > 60 else s[:60]

        ps = PorterStemmer()
        words = [ps.stem(w) for w in word_tokenize(s.strip(OLDumpParser.punctuation_without_braces + whitespace))]

        # Join the words back together until max_length is reached
        shortened = ""
        for w in words:
            if len(shortened) + len(w) + 1 > max_length:  # +1 for space
                break
            shortened += w + " "

        return shortened.rstrip()