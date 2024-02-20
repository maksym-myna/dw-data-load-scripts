import concurrent.futures

import gdrive.auth as auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from data_processor import DataProcessor

class JSONLDataProcessor(DataProcessor):
    def __init__(selfs):
        super().__init__('jsonl')

    def run(self, directory = r'open library dump') -> None:
        """Main function to process files and upload to Google Drive."""
        # auth = gauth.GDriveAuth()
        
        # cls.download_and_unarchive_datasets()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            list_of_file_lists = {executor.submit(parser.process_latest_file, directory)
                                for parser in self.ol_parsers}
            list_of_file_lists.add(executor.submit(self.sl_parser.process_file,
                                r'seattle library dump\checkouts.json',
                                        r'seattle library dump\data\checkout.json'))

            for future in concurrent.futures.as_completed(list_of_file_lists):
                try:
                    service = build("drive", "v3", credentials=auth.creds)
                    files = future.result()
                    DataProcessor.upload_to_drive(service, files)
                    DataProcessor.delete_file(*files)
                except Exception:
                    return
                    
    @staticmethod
    def upload_to_drive(service, files: list[str]) -> None:
        """Upload files to Google Drive and handle revisions."""
        for file_location in files:
            file_name = file_location.split('\\')[-1]
            search = service.files().list(q=f"name='{file_name}'").execute()
            media_body = MediaFileUpload(file_location, mimetype='application/json',
                                        resumable=True)
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
            service.revisions().delete(fileId=id, revisionId=versions
                                    ['revisions'][0]['id']).execute()