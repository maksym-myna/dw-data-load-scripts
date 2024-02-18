import concurrent.futures

import gdrive.auth as gauth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import parsers.ol_readings_parser as olreadsp
import parsers.ol_ratings_parser as olratesp
import parsers.ol_dump_parser as oldumpp
import parsers.sl_dump_parser as sldumpp

import os
import requests
import gzip

def download_file(url, download_path):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    downloaded_size = 0

    with open(download_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024): 
            if chunk:  # filter out keep-alive new chunks
                downloaded_size += len(chunk)
                f.write(chunk)
                print(f"Download progress: {100 * downloaded_size / total_size:.2f}%")

    return download_path

def unarchive_file(archive_path, unarchive_path):
    # Extract the original file name
    original_file_name = os.path.splitext(os.path.basename(archive_path))[0]
    total_size = os.path.getsize(archive_path)
    unarchived_size = 0
    
    # Unpack the file in chunks
    with gzip.open(archive_path, 'rb') as f_in:
        with open(os.path.join(unarchive_path, original_file_name), 'wb') as f_out:
            while True:
                chunk = f_in.read(1024*1024*256)  # read 256MB at a time
                if not chunk:
                    break
                unarchived_size += len(chunk)
                f_out.write(chunk)
                print(f"Unarchival progress: {100 * unarchived_size / total_size:.2f}%")

    return unarchive_path

def delete_file(*path):
    for file in path:
        os.remove(file)


def upload_to_drive(service, files):
    """Upload files to Google Drive and handle revisions."""
    for file_location in files:
        file_name = file_location.split('\\')[-1]
        search = service.files().list(q=f"name='{file_name}'").execute()
        media_body = MediaFileUpload(file_location, mimetype='application/json', resumable=True)
        file = search.get('files')[0]
        id = file.get('id')

        request = service.files().update(
            fileId=id,
            body={},
            media_body=media_body
        )
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"{file_name} is {int(status.progress() * 100)}% uploaded.")
        print("Uploaded!")
        versions = service.revisions().list(fileId=id).execute()
        service.revisions().delete(fileId=id, revisionId=versions['revisions'][0]['id']).execute()


def main():
    """Main function to process files and upload to Google Drive."""
    auth = gauth.GDriveAuth()
    ol_parsers = [
        oldumpp.OLDumpParser(),
        olratesp.OLRatingsParser(),
        olreadsp.OLReadingsParser(),
    ]
    sl_parser = sldumpp.SLDataParser()
    ol_files = {
    'https://openlibrary.org/data/ol_dump_latest.txt.gz' : 'open library dump/ol_dump_latest.txt.gz',
    'https://openlibrary.org/data/ol_dump_ratings_latest.txt.gz' : 'open library dump/ol_dump_ratings_latest.txt.gz',
    'https://openlibrary.org/data/ol_dump_reading-log_latest.txt.gz' : 'open library dump/ol_dump_reading-log_latest.txt.gz'
    }
    sl_files = {
        'https://data.seattle.gov/resource/tmmm-ytt6.json?$query=SELECT%20`materialtype`,%20`checkoutyear`,%20`checkoutmonth`,%20`checkouts`,%20`title`,%20`isbn`%20WHERE%20(`isbn`%20IS%20NOT%20NULL)%20AND%20caseless_one_of(%20`materialtype`,%20%22BOOK,%20ER%22,%20%22BOOK%22,%20%22AUDIOBOOK%22,%20%22EBOOK%22%20)%20ORDER%20BY%20`title`%20DESC%20NULL%20LAST,%20`isbn`%20DESC%20NULL%20LAST%20LIMIT%202147483647'
        : 'seattle library dump/checkouts.json'
    }
    directory = r'open library dump'

    for url, download_path in ol_files.items():
        archive = download_file(url, download_path)
        unarchive_file(archive, 'open library dump/')
    
    for url, download_path in sl_files.items():
        archive = download_file(url, download_path)
        
    with concurrent.futures.ThreadPoolExecutor() as executor:
        list_of_file_lists = {executor.submit(parser.process_latest_file, directory) for parser in ol_parsers}
        list_of_file_lists.add(executor.submit(sl_parser.process_file, r'seattle library dump\checkouts.json', r'seattle library dump\data\seattle_library.json'))

        for future in concurrent.futures.as_completed(list_of_file_lists):
            try:
                service = build("drive", "v3", credentials=auth.creds)
                files = future.result()
                upload_to_drive(service, files)
                delete_file(*files)
            except Exception as e:
                print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()