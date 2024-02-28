from abc import ABC, abstractmethod

from parsers.abstract_parser import AbstractParser
import parsers.ol_readings_parser as olreadsp
import parsers.ol_ratings_parser as olratesp
import parsers.ol_dump_parser as oldumpp
import parsers.sl_dump_parser as sldumpp

import os
import requests
import gzip

class DataProcessor(ABC):
    def __init__(self, file_type: str) -> None:
        self.ol_parsers = [
            oldumpp.OLDumpParser(file_type),
            olratesp.OLRatingsParser(file_type),
            olreadsp.OLReadingsParser(file_type),
        ]
        self.sl_parser = sldumpp.SLDataParser(file_type)
        self.ol_files = {
            'https://openlibrary.org/data/ol_dump_latest.txt.gz' :
                'open library dump/ol_dump_latest.txt.gz',
            'https://openlibrary.org/data/ol_dump_ratings_latest.txt.gz' :
                'open library dump/ol_dump_ratings_latest.txt.gz',
            'https://openlibrary.org/data/ol_dump_reading-log_latest.txt.gz' :
                'open library dump/ol_dump_reading-log_latest.txt.gz'
            }
        self.sl_files = {
            'https://data.seattle.gov/resource/tmmm-ytt6.json?$query=SELECT%20'
            '`materialtype`,%20`checkoutyear`,%20`checkoutmonth`,%20`checkouts`,%20`title'
            '`,%20`isbn`%20WHERE%20(`isbn`%20IS%20NOT%20NULL)%20AND%20caseless_one_of(%20'
            '`materialtype`,%20%22BOOK,%20ER%22,%20%22BOOK%22,%20%22AUDIOBOOK%22,%20'
            '%22EBOOK%22%20)ORDER%20BY%20`title`%20DESC%20NULL%20LAST,%20`isbn`%20DESC'
            '%20NULL%20LAST%20LIMIT%202147483647' : 'seattle library dump/checkouts.json'
        }
    
    @abstractmethod
    def run(cls, directory = r'open library dump') -> None:
        pass
    
    @staticmethod
    def download_file(url: str, download_path: str) -> str:
        """
        Downloads a file from the given URL and saves it to the specified download path.

        Args:
            url (str): The URL of the file to be downloaded.
            download_path (str): The path where the downloaded file will be saved.

        Returns:
            str: The path of the downloaded file.

        """
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0

        if not AbstractParser.is_path_valid(download_path):
            raise NotADirectoryError(download_path)

        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:  # filter out keep-alive new chunks
                    downloaded_size += len(chunk)
                    f.write(chunk)
                    print(f"Download progress: {100 * downloaded_size / total_size:.2f}%")

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

        if not AbstractParser.is_path_valid(archive_path) or \
                not AbstractParser.is_path_valid(unarchive_path):
            raise NotADirectoryError(archive_path)

        with gzip.open(archive_path, 'rb') as f_in, \
                open(os.path.join(unarchive_path, original_file_name), 'wb') as f_out:
            while True:
                chunk = f_in.read(1024*1024*256)  # read 256MB at a time
                if not chunk:
                    break
                unarchived_size += len(chunk)
                f_out.write(chunk)
                print(f"Unarchival progress: {100 * unarchived_size / total_size:.2f}%")

        return unarchive_path

    @staticmethod
    def delete_file(*path : str) -> None:
        """
        Deletes the specified files.

        Args:
            *path: Variable number of file paths to be deleted.

        Returns:
            None
        """
        for file in path:
            os.remove(file)
    
    @classmethod
    def download_and_unarchive_datasets(cls) -> None:
        for url, download_path in cls.ol_files.items():
            archive = DataProcessor.download_file(url, download_path)
            DataProcessor.unarchive_file(archive, 'open library dump/')

        for url, download_path in cls.sl_files.items():
            archive = DataProcessor.download_file(url, download_path)