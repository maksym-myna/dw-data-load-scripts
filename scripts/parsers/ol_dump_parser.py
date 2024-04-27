import string
from parsers.ol_abstract_parser import OLAbstractParser
from parsers.user_manager import UserManager
from .abstract_parser import AbstractParser
from parsers.file_writer import FileWriter

from string import punctuation, whitespace, capwords
from typing import Callable, Dict, List, Set
from lingua import LanguageDetectorBuilder
from orjson import loads as jsonloads
from transliterate import translit
from functools import lru_cache
from datetime import datetime
from html import unescape
from io import StringIO

from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet as wn
from nltk.stem import PorterStemmer
from nltk import download

import fasttext.util
import pandas as pd
import numpy as np
import itertools
import sqlite3
import random
import csv
import os
import re


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

    UNKNOWN_PUBLISHER_NAME = "Unknown"
    UNKNOWN_PUBLISHER = [UNKNOWN_PUBLISHER_NAME]
    BRACELESS_PUNCTUIATION = punctuation.translate(str.maketrans("", "", "(){}[]"))
    BRACELESS_PUNCTUIATION_WITH_SPACE = BRACELESS_PUNCTUIATION + whitespace
    REMOVALS_PATTERN = re.compile(
        "|".join(
            map(
                re.escape,
                ["&NewLine", "&#13", "&#10", "&#10;", "&#13;", '"', "&quot;", "\\"],
            )
        )
    )
    OPENING_TO_CLOSING_PARENTHESES = {"(": ")", "[": "]", "{": "}"}
    CLOSING_TO_OPENING_PARENTHESES = {
        v: k for k, v in OPENING_TO_CLOSING_PARENTHESES.items()
    }

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

        fasttext.util.download_model("en", if_exists="ignore")
        self.ft = fasttext.load_model("cc.en.300.bin")

        self.__type_mapping: Dict[str, Callable] = {
            "edition": self.__process_edition,
            "work": self.__process_work,
            "author": self.__process_author,
        }

        self.__normalized_types: List[str] = [
            "publisher",
            "work",
            "author",
            "work_author",
            "subject",
            "work_subject",
        ]

        self.__subjects_to_themes: Dict[str, List[str]] = {
            "Action & Adventure": ["action", "adventure", "thrill", "exciting", "heroic" "quest", "fight", "treasure"],
            "Fantasy": ["magic", "fantasy", "wizard", "dragon"],
            "Science Fiction": ["space", "alien", "technology", "future", "robot"],
            "Dystopian": ["dystopia", "apocalypse", "totalitarian", "rebellion"],
            "Mystery": ["detective", "crime", "mystery"],
            "Horror": ["horror", "scary", "supernatural", "haunted", "monsters"],
            "Romance": ["love", "relationship", "heartbreak", "passion", "soulmate"],
            "LGBTQ+": ["lgbtq", "gay", "lesbian", "queer", "identity"],
            "Contemporary Fiction": ["modern", "realistic", "current", "society"],
            "Young Adult": ["young", "teen", "adolescent", "coming-of-age"],
            "Graphic Novel": ["graphic", "comic", "illustrated", "visual"],
            "Children's": ["children", "kids", "friendship", "lessons"],
            "Biography": ["biography", "life", "person", "memoir"],
            "Food & Drink": ["food", "drink", "cooking", "cuisine", "recipe"],
            "Art & Photography": ["art", "photography", "visual", "creative"],
            "Self-help, guide & how-to": ["help", "impovement", "advice", "motivation"],
            "History & Travel": ["history", "travel", "culture", "explore"],
            "True Crime": ["crime", "investigation", "murder", "justice", "victim"],
            "Religion & Spirituality": ["religion", "spirituality", "faith"],
            "Humanities & Social Sciences": ["humanities","society","behavior","identity"],
            "Science & Technology": ["science", "technology", "research"],
        }
        self.__subjects = list(self.__subjects_to_themes.keys())
        self.__themes = [
            theme for sublist in self.__subjects_to_themes.values() for theme in sublist
        ]
        self.__words_vectors = {
            w: self.ft.get_sentence_vector(w) for w in self.__themes
        }

        self.__subject_ids: dict[str, int] = {
            key: index for index, key in enumerate(self.__subjects)
        }
        self.__themes_to_subjects = {
            theme: subject
            for subject, themes in self.__subjects_to_themes.items()
            for theme in themes
        }

        self.__language_mapping: dict[str, str | None] = {
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
            "mao": "mri",
            "scr": "hrv",
            "esp": "epo",
            "eth": "gez",
            "far": "fao",
            "fri": "fry",
            "gag": "glg",
            "gua": "grn",
            "iri": "gle",
            "cam": "khm",
            "mla": "mlg",
            "lan": "oci",
            "gal": "orm",
            "lap": "smi",
            "sao": "smo",
            "scc": "srp",
            "snh": "sin",
            "sho": "sna",
            "sso": "sot",
            "swz": "ssw",
            "tag": "tgl",
            "tgk": "taj",
            "tar": "tat",
            "tsw": "tsn",
            "int": "ina",
            "scr": "hrv"
        }
        
        self.__ukrainian_letters_mapping = {
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

        self.__language_detector = (
            LanguageDetectorBuilder.from_all_languages()
            .with_low_accuracy_mode()
            .build()
        )

        self.__output_files = None

        self.__work_id = itertools.count(1)
        self.__publisher_id = itertools.count(1)
        self.__author_id = itertools.count(1)

        self.__author_ids = {}
        self.__work_authors = {}
        self.__publishers = {}
        self.mapped_work_ids = {}
        self.work_ids = set()

        self.cursor.executescript(
            open("scripts/sql/sqlite_schema.sql", "r", encoding="utf-8").read()
        )

        wn.ensure_loaded()

    def process_file(
        self, input_file: str, output_file: str | None = None
    ) -> list[str]:
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

        PROCESS_EVERY_NTH_VALUE = 10
        CHUNK_SIZE = 1000
        TO_SKIP = CHUNK_SIZE * (PROCESS_EVERY_NTH_VALUE - 1)

        regex = re.compile(r"[\n\r]")
        with open(input_file, "r", encoding="utf-8") as f_in:
            print(f"Reading file '{input_file}' - {datetime.now().isoformat()}")
            while True:
                lines = list(itertools.islice(f_in, TO_SKIP, TO_SKIP + CHUNK_SIZE))
                if not lines:
                    break
                for line in lines:
                    split = line.split("\t")
                    obj = jsonloads(split[4])

                    for key, value in obj.items():
                        if isinstance(value, str):
                            obj[key] = regex.sub(" ", value)

                    if func := self.__type_mapping.get(
                        self.parse_id(obj.get("type")["key"]), None
                    ):
                        try:
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
        print(f"Processing publishers - {datetime.now().isoformat()}")
        self.__write_publishers()
        print(f"Processing authors - {datetime.now().isoformat()}")
        self.__write_authors()
        print(f"Processing subjects - {datetime.now().isoformat()}")
        self.__write_subjects()

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
        old_id = next(
            self.parse_id(work["key"]) for work in obj.get("works", [{"key": ""}])
        )
        return self.__get_new_id(old_id, self.mapped_work_ids, self.__work_id)

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

        if not isbns:
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
        publisher = capwords(self.__html_escape(
            obj.get("publishers", OLDumpParser.UNKNOWN_PUBLISHER)[0]
        ))
        if ukrainian_flag and publisher != OLDumpParser.UNKNOWN_PUBLISHER_NAME:
            publisher = self.transliterate_to_ukrainian(publisher, publisher=True)

        try:
            if not (publisher_id := self.__publishers.get(publisher)) and (
                publisher_id := next(self.__publisher_id)
            ):
                self.__publishers[publisher] = publisher_id
        except StopIteration:
            return None

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
        number_of_pages = abs(obj.get("number_of_pages", random.randint(128, 512)))

        if published_at := obj.get("publish_date"):
            published_at = self.__find_year(published_at)
        if not published_at:
            published_at = random.randint(1900, 2022)

        if weight := obj.get("weight"):
            weight = self.__find_weight_in_kg(weight)
        if not weight:
            weight = self.__calculate_weight(number_of_pages)

        self.__insert_authors(obj, work_id)

        self._write_strategy(
            self.__output_files["work"],
            {
                "work_id": work_id,
                "publisher_id": publisher_id,
                "isbn": isbn,
                "language": language,
                "title": title,
                "number_of_pages": number_of_pages,
                "weight": weight,
                "published_at": published_at,
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
            capwords(self.__html_escape(subject))
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
        work_authors = {
            self.parse_id(a)
            for author in obj.get("authors", [])
            if (a := (author.get("author") or author).get("key"))
        }
        author_ids = {
            int(self.__get_new_id(old_id, self.__author_ids, self.__author_id))
            for old_id in work_authors
        }

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
        work_id = self.__get_new_id(old_id, self.mapped_work_ids, self.__work_id)

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

        self._write_strategy(
            self.__output_files["author"],
            {"author_id": author_id, "name": name, "modified": created},
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
        s = OLDumpParser.REMOVALS_PATTERN.sub("", s)
        s = unescape(s)
        return s.strip(OLDumpParser.BRACELESS_PUNCTUIATION_WITH_SPACE).replace('"', "'")

    def __build_title(self, obj: dict) -> str:
        """
        Build the title string based on the given object.

        Args:
            obj (dict): The object containing the title information.

        Returns:
            str: The built title string.
        """
        title = "{}. {}: {} {}".format(
            obj.get("title_prefix", ""),
            obj.get("title", ""),
            obj.get("subtitle", "")
        )
        title = self.__html_escape(title)
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
            if not (new_id := id_dict.get(old_id)):
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

        if result := list(pattern.finditer(s)):
            if "k" in s:
                conversion_factor = 1
            elif "g" in s:
                conversion_factor = 1 / 1000
            elif "z" in s or "ounc" in s:
                conversion_factor = 1 / 35.284
            elif "lb" in s or "pound" in s:
                conversion_factor = 1 / 2.205
            else:
                conversion_factor = 1

            result = float(result[0].group()) * conversion_factor

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

    def __write_authors(self) -> None:
        """
        Writes the given dictionary of sets to the output file with the specified name.

        Args:
            dictionary (dict[int, set]): The dictionary of sets to be written.
            name (str): The name of the output file.

        Returns:
            None
        """

        work_authors = set()
        work_ids: Set[int] = set(
            row[0] for row in self.cursor.execute("SELECT work_id FROM work_id")
        )
        
        AUTHOR_LOCATION = rf"open library dump\data\author.{self.type_name}"
        NEW_AUTHOR_LOCATION = rf"open library dump\data\new_author.{self.type_name}"
        for chunk in pd.read_csv(
                AUTHOR_LOCATION,
                names=[
                    "author_id",
                    "full_name",
                    "modified_at"
                ],
                chunksize=500_000,
            ):
                author_ids_set = set(chunk['author_id'])

                for work_id, author_ids in self.__work_authors.items():
                    if work_id not in work_ids:
                        continue
                    for author_id in author_ids:
                        if author_id in author_ids_set:
                            work_authors.add((work_id, author_id))

                self._tuple_write_strategy(
                    self.__output_files["work_author"],
                    [
                        (work_id, author_id, datetime.now().isoformat())
                        for work_id, author_id in work_authors
                    ],
                )
                del self.__work_authors

                author_ids = { author_id for _, author_id in work_authors }
    
                chunk = chunk[chunk['author_id'].isin(author_ids)]
                chunk.to_csv(
                    NEW_AUTHOR_LOCATION,
                    mode="a",
                    index=False,
                    header=False,
                    encoding="utf-8",
                    quoting=csv.QUOTE_ALL,
                )
        self.__output_files["author"].close()
        os.remove(AUTHOR_LOCATION)
        os.rename(NEW_AUTHOR_LOCATION, AUTHOR_LOCATION)

    def process_name(
        self, s: str, title: bool = False, capitalize_first: bool = False
    ) -> str:
        stack = []
        matched_chars = StringIO()
        for c in s:
            if c in OLDumpParser.OPENING_TO_CLOSING_PARENTHESES:
                stack.append(c)
            elif (
                c in OLDumpParser.CLOSING_TO_OPENING_PARENTHESES
                and stack
                and stack[-1] == OLDumpParser.CLOSING_TO_OPENING_PARENTHESES[c]
            ):
                stack.pop()
            elif not stack or stack[
                -1
            ] != OLDumpParser.CLOSING_TO_OPENING_PARENTHESES.get(c):
                matched_chars.write(c)

        # Remove unmatched characters from s
        s = matched_chars.getvalue()

        # Remove trailing punctuation and replace multiple spaces with a single space
        s = re.sub(r"[{}]+\s*|\s+", " ", s).strip()

        # Decide the transformation function based on title and capitalize_first
        s = s.title() if title else self.capitalize_first(s) if capitalize_first else s

        return s

    def __write_publishers(self) -> None:
        """
        Writes the publishers to the output file.

        Returns:
            None
        """
        WORK_LOCATION = rf"open library dump\data\work.{self.type_name}"
        NEW_WORK_LOCATION = rf"open library dump\data\new_work.{self.type_name}"
        PUBLISHER_LOCATION = rf"open library dump\data\publisher.{self.type_name}"
        SHORTENED_STRING_MAX_LENGTH = 50

        with open(PUBLISHER_LOCATION, "w", encoding="utf-8", newline="") as file:
            publisher_names = {
                name: self.preprocess_publisher(
                    self.shorten_string(name, SHORTENED_STRING_MAX_LENGTH)
                )
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
                short_name = (
                    self.process_name(
                        self.shorten_string(name, SHORTENED_STRING_MAX_LENGTH),
                        title=True,
                    )
                    or OLDumpParser.UNKNOWN_PUBLISHER_NAME
                )
                if new_publishers.get(short_name):
                    publisher_collisions[id] = new_publishers[short_name]
                else:
                    new_publishers[short_name] = id

            unknown_id = max(map(int, new_publishers.values())) + 1
            if not new_publishers.get(OLDumpParser.UNKNOWN_PUBLISHER_NAME):
                new_publishers[OLDumpParser.UNKNOWN_PUBLISHER_NAME] = unknown_id
            else:
                unknown_id = new_publishers.get(OLDumpParser.UNKNOWN_PUBLISHER_NAME)

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

            # CHUNK_SIZE = 1000

            publisher_ids = set()
            read_works = pd.read_csv(
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
                dtype={"isbn": str}
            )
            read_works = read_works.drop_duplicates(subset=['work_id'])
            read_works = read_works.drop_duplicates(subset=['isbn'])
            read_works = read_works.dropna(subset=["title"])
            read_works["publisher_id"] = read_works["publisher_id"].apply(
                lambda old_id: old_to_new_ids[old_id]
            )
            work_ids = read_works["work_id"].apply(lambda x: (x,)).tolist()
            self.cursor.executemany(
                "INSERT OR IGNORE INTO work_id VALUES (?)", work_ids
            )
            publisher_ids.update(read_works["publisher_id"].dropna().values)
            read_works.to_csv(
                NEW_WORK_LOCATION,
                mode="a",
                index=False,
                header=False,
                encoding="utf-8",
                quoting=csv.QUOTE_ALL,
            )
            self._tuple_write_strategy(
                file, [(pid, new_publishers[pid], datetime.now().isoformat()) for pid in publisher_ids]
            )

        self.__output_files["work"].close()
        os.remove(WORK_LOCATION)
        os.rename(NEW_WORK_LOCATION, WORK_LOCATION)

    def preprocess_publisher(self, name: str) -> str:
        name = name.lower()
        s = re.sub(
            r"\b(імені|имени|ім|им|університет|інститут|національний|національна|нац|державний|державна|у|і|й|college|institute|textbook|textbooks|books|group|incorporated|inc|house|illustrate|australia|united|states|usa|us|uk|canada|india|publications|pubns|publisher|publishers|publishing|pub|primary|secondary|collection|collections|communication|communications|company|companies|co|imprints|imprint|reprints|reprint|imprints|prints|print|press|ltd|juvenile|juv|professional|pro|pr|school|educational|education|division|library|interest|international|int|intrnl|intl|intnl|literature|txt|general|management|hall|academy|academies|academic|learning|paperbacks|paperback|bros|home|audio|video|multimedia|media|story|stories|writer|writers|org|organization|org|panamerican|american|america|departments|department|archives|archive|editions|edition|editorials|editorial|trade|press|biblioteked|biblioteka|biblioteke|bbc|junior|with|for|llc|vydvo|vyd-vo|edu|gp|news|university|univ|uni|t-vo|tvo|tovarystvo|government|govt|gov|the|in|of|at|an|a|de|et)\b",
            "",
            name,
        )
        try:
            if not s:
                words = name.split()
                articles = {
                    "the",
                    "a",
                    "an",
                    "de",
                    "et",
                    "in",
                    "of",
                    "at",
                    "у",
                    "і",
                    "й",
                }
                name = next(
                    (word for word in words if word.lower() not in articles),
                    words[0] if words else OLDumpParser.UNKNOWN_PUBLISHER_NAME,
                )
        except Exception:
            pass
        s = re.sub(r"\W+", "", s, flags=re.UNICODE)
        return s.strip()

    def __write_subjects(self) -> None:
        """
        Writes the subjects to the output file.

        Returns:
            None
        """
        work_subject_names = self.cursor.execute(
            f"""
            SELECT work_id.work_id, subject_name
            FROM work_id
            LEFT JOIN work_subject ON work_subject.work_id = work_id.work_id
            """
        ).fetchall()
        work_subject_names = [
            (id, name)
            for id, name in work_subject_names
        ]

        self._tuple_write_strategy(
            self.__output_files["subject"],
            [(id, name, datetime.now().isoformat()) for name, id in self.__subject_ids.items()],
        )

        subjects = set()
        work_subjects = set()
        for work_id, subject_name in work_subject_names:
            if subject_name:
                subject_name = self.__find_subject_by_theme(self.preprocess(subject_name))
                subject_id = self.__subject_ids[subject_name]
            else:
                subject_name = random.choice(self.__subjects)
                subject_id = self.__subject_ids[subject_name]                
            subjects.add((subject_name, subject_id))
            work_subjects.add((work_id, subject_id))

        self._tuple_write_strategy(self.__output_files["work_subject"], work_subjects)


    def transliterate_to_ukrainian(self, text: str, publisher=False) -> str:
        """
        Transliterates the given text from a Latin-based script to Ukrainian Cyrillic script.

        Args:
            text (str): The text to be transliterated.
            publisher (bool, optional): Whether the text is a publisher name. Defaults to False.

        Returns:
            str: The transliterated text.

        """
        for original, replacement in self.__ukrainian_letters_mapping.items():
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
        text = re.sub(
            r"[^\w\s]",
            "",
            text.strip(OLDumpParser.BRACELESS_PUNCTUIATION_WITH_SPACE),
        )
        text = re.sub(r"([бпвмфгкхжчшрБПВМФГКХЖЧШР])ь", r"\1", text)
        text = re.sub(r"([бпвмфгкхжчшр])([яюєї])", r"\1\'\2", text)

        return text

    def preprocess(self, text: str) -> str:
        """
        Preprocesses the given text by performing the following steps:
        1. Lowercases the text and removes punctuation.
        2. Tokenizes the text.

        Args:
            text (str): The text to be preprocessed.

        Returns:
            str: The preprocessed text.

        """
        text = text.lower()
        text = re.sub(r"[^\w\s]", "", text)
        words = text.split(' ')
        return " ".join(words)

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
        s = s.split("by", 1)[0]
        if len(s) <= max_length:
            return s

        match = re.search(r"\\u\+\d{4}", s[:60])
        s = s[: match.start()] if match and match.end() > 60 else s[:60]

        ps = PorterStemmer()
        words = [
            ps.stem(w)
            for w in word_tokenize(
                s.strip(OLDumpParser.BRACELESS_PUNCTUIATION_WITH_SPACE)
            )
        ]

        # Join the words back together until max_length is reached
        shortened = " ".join(w for w in words if len(w) + 1 <= max_length)

        return shortened.rstrip()

    @staticmethod
    def cos_sim(a: np.ndarray, b: np.ndarray) -> float:
        """Takes 2 vectors a, b and returns the cosine similarity according
        to the definition of the dot product
        """
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    @lru_cache(maxsize=None)
    def __find_subject_by_theme(self, w: str) -> str:
        """
        Compares new word with those in the words vectors dictionary
        """
        vec = self.ft.get_sentence_vector(w)
        max_sim_word = None
        max_sim = -1

        for w1, vec1 in self.__words_vectors.items():
            sim = self.cos_sim(vec, vec1)
            if sim > max_sim:
                max_sim = sim
                max_sim_word = w1
        
        if not max_sim_word:
            max_sim_word = random.choice(self.themes)

        return self.__themes_to_subjects[max_sim_word]
