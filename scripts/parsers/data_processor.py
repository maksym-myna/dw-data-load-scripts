from abc import ABC, abstractmethod
import sqlite3

from parsers.abstract_parser import AbstractParser
import parsers.ol_reads_rates_parser as olrrsp
import parsers.ol_dump_parser as oldumpp
import parsers.sl_dump_parser as sldumpp

import os
import requests
import gzip

from parsers.user_manager import UserManager


class DataProcessor(ABC):
    """
    Abstract base class for data processing.

    Args:
        file_type (str): The type of file to be processed.

    Attributes:
        user_manager (UserManager): An instance of the UserManager class.
        old_parser (OLDumpParser): An instance of the OLDumpParser class.
        ol_parsers (list[AbstractParser]): A list of instances of the
            OLRatingsParser and OLReadingsParser classes.
        sl_parser (SLDataParser): An instance of the SLDataParser class.
        ol_files (dict): A dictionary of Open Library files URLs.
        sl_files (dict): A dictionary of Seattle Library files URLs.

    Methods:
        run(self, directory=r'open library dump'):
            Abstract method to run the data processing.
        download_file(url: str, download_path: str) -> str:
            Downloads a file from a URL.
        unarchive_file(archive_path: str, unarchive_path: str) -> str:
            Unarchives a gzip compressed file.
        delete_file(*path: str) -> None: Deletes the specified files.
        download_and_unarchive_datasets(self) -> None:
            Downloads and unarchives the datasets.
    """

    def __init__(self, file_type: str) -> None:
        """
        Initializes a DataProcessor object.

        Args:
            file_type (str): The type of file to be processed.

        Returns:
            None
        """
        self.sqlite_conn = sqlite3.connect("temp.db")
        self.type_name = file_type
        self.user_manager = UserManager(file_type)

        self.old_parser = oldumpp.OLDumpParser(
            self.sqlite_conn, file_type, self.user_manager
        )
        self.ol_parsers = [
            olrrsp.OLRRParser(
                self.sqlite_conn, file_type, self.user_manager, "listing"
            ),
            olrrsp.OLRRParser(self.sqlite_conn, file_type, self.user_manager, "rating"),
        ]
        self.sl_parser = sldumpp.SLDataParser(
            self.sqlite_conn, file_type, self.user_manager
        )
        self.ol_files = {
            "https://openlibrary.org/data/ol_dump_latest.txt.gz": "open library dump/ol_dump_latest.txt.gz",
            "https://openlibrary.org/data/ol_dump_ratings_latest.txt.gz": "open library dump/ol_dump_ratings_latest.txt.gz",
            "https://openlibrary.org/data/ol_dump_reading-log_latest.txt.gz": "open library dump/ol_dump_reading-log_latest.txt.gz",
        }
        self.sl_files = {
            "https://data.seattle.gov/resource/tmmm-ytt6.json?$query=SELECT%20"
            "`materialtype`,%20`checkoutyear`,%20`checkoutmonth`,%20`checkouts`,%20`title"
            "`,%20`isbn`%20WHERE%20(`isbn`%20IS%20NOT%20NULL)%20AND%20caseless_one_of(%20"
            "`materialtype`,%20%22BOOK,%20ER%22,%20%22BOOK%22,%20%22AUDIOBOOK%22,%20"
            "%22EBOOK%22%20)ORDER%20BY%20`title`%20DESC%20NULL%20LAST,%20`isbn`%20DESC"
            "%20NULL%20LAST%20LIMIT%202147483647": "seattle library dump/checkouts.json"
        }

    def __del__(self) -> None:
        """
        Closes the connection to the database.

        Returns:
            None
        """
        self.sqlite_conn.close()
        # os.remove("temp.db")

    @abstractmethod
    def run(self, directory=r"open library dump") -> None:
        """
        Process the data in the specified directory.

        Args:
            directory (str): The directory containing the data to be processed.
                Defaults to 'open library dump'.
        """
        pass

    @staticmethod
    def download_file(url: str, download_path: str) -> str:
        """
        Downloads a file from the URL and saves it to the download path.

        Args:
            url (str): The URL of the file to be downloaded.
            download_path (str): The path to save the downloaded file.

        Returns:
            str: The path of the downloaded file.

        """
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get("content-length", 0))
        downloaded_size = 0

        if not AbstractParser.is_path_valid(download_path):
            raise NotADirectoryError(download_path)

        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:  # filter out keep-alive new chunks
                    downloaded_size += len(chunk)
                    f.write(chunk)
                    print(
                        f"Download progress: {100 * downloaded_size / total_size:.2f}%"
                    )

        return download_path

    @staticmethod
    def unarchive_file(archive_path: str, unarchive_path: str) -> str:
        """
        Unarchives a gzip compressed file.

        Args:
            archive_path (str): The path to the gzip compressed file.
            unarchive_path (str): The path to store the unarchived file.

        Returns:
            str: The path to the unarchived file.
        """
        # Extract the original file name
        original_file_name = os.path.splitext(os.path.basename(archive_path))[0]
        total_size = os.path.getsize(archive_path)
        unarchived_size = 0

        if not AbstractParser.is_path_valid(
            archive_path
        ) or not AbstractParser.is_path_valid(unarchive_path):
            raise NotADirectoryError(archive_path)

        with gzip.open(archive_path, "rb") as f_in, open(
            os.path.join(unarchive_path, original_file_name), "wb"
        ) as f_out:
            while True:
                chunk = f_in.read(1024 * 1024 * 256)  # read 256MB at a time
                if not chunk:
                    break
                unarchived_size += len(chunk)
                f_out.write(chunk)
                print(f"Unarchival progress: {100 * unarchived_size / total_size:.2f}%")

        return unarchive_path

    @staticmethod
    def delete_file(*path: str) -> None:
        """
        Deletes the specified files.

        Args:
            *path: Variable number of file paths to be deleted.

        Returns:
            None
        """
        for file in path:
            os.remove(file)

    def download_and_unarchive_datasets(self) -> None:
        """
        Downloads and unarchives datasets from the specified URLs.

        This method iterates over the `ol_files` and `sl_files` dictionaries,
        where the keys are the URLs of the datasets to be downloaded, and the
        values are the paths where the downloaded files will be saved.

        For each URL and download path, it downloads the file using the
        `download_file` method and then unarchives it using the `unarchive_file`
        method.

        Note: The unarchived files are saved in the 'open library dump/' directory.

        Returns:
            None
        """
        for url, download_path in self.ol_files.items():
            archive = DataProcessor.download_file(url, download_path)
            DataProcessor.unarchive_file(archive, "open library dump/")

        for url, download_path in self.sl_files.items():
            archive = DataProcessor.download_file(url, download_path)
