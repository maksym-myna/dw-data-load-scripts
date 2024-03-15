import concurrent.futures
import time
from data_processor import DataProcessor
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient import discovery, errors
import os

class CSVDataprocessor(DataProcessor):
    def __init__(self):
        super().__init__('csv')
        self.blobs = []
    
    def __del__(self):
        for blob in self.blobs:
            blob.delete()

    def run(self, old_directory = r'open library dump', sld_directory = r'seattle library dump') -> None:
        # cls.download_and_unarchive_datasets()

        # files = self.old_parser.process_latest_file(old_directory)
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     files.extend([executor.submit(parser.process_latest_file, self.old_parser.work_ids, old_directory)
        #                         for parser in self.ol_parsers])
        # files.extend(self.sl_parser.process_file(rf'{sld_directory}\checkouts.json',
        #     [rf'{sld_directory}\data\loan.{self.type_name}', rf'{sld_directory}\data\loan_return.{self.type_name}']))

        # self.conn.close()
        # self.user_manager.writeUsers()
        # return

        old = r'open library dump\data'
        sld = r'seattle library dump\data'
        files = [
        #     rf'{old}\lang.{self.type_name}',
        #     rf'{old}\library_user.{self.type_name}',
        #     rf'{old}\pfp.{self.type_name}',
        #     rf'{old}\publisher.{self.type_name}',
        #     rf'{old}\work.{self.type_name}',
        #     rf'{old}\work_isbn.{self.type_name}',
        #     rf'{old}\work_language.{self.type_name}',
        #     rf'{old}\work_weight.{self.type_name}',
        #     rf'{old}\author.{self.type_name}',
        #     rf'{old}\work_author.{self.type_name}',
        #     rf'{old}\subject.{self.type_name}',
             rf'{old}\work_subject.{self.type_name}',
             rf'{old}\rating.{self.type_name}',
             rf'{old}\listing.{self.type_name}',
             rf'{sld}\inventory_item.{self.type_name}',
             rf'{sld}\loan.{self.type_name}',
             rf'{sld}\loan_return.{self.type_name}',
        ]

        try:
            project_id = 'library-414223'
            instance_id = 'library'
            database = 'library'
            bucket_name = 'data_warehousing_library_data'
            
            credentials_location = r'scripts\cloudsql\credentials.json'
            credentials = service_account.Credentials.from_service_account_file(credentials_location)
            service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
            
            storage_client = storage.Client.from_service_account_json(credentials_location)
            bucket = storage_client.get_bucket(bucket_name)

            blobs = []
            for file in files:
                filename = file.split('\\')[-1]
                blob = bucket.blob(filename)
                
                with open(file, 'rb') as f:
                    blob.upload_from_file(f)
                self.blobs.append(blob) 
                table_name = filename.split('.')[0]

                file_uri = f'gs://{bucket_name}/{filename}'

                import_request_body = {
                    "importContext": {
                        "fileType": "CSV",
                        "uri": file_uri,
                        "database": database,
                        "csvImportOptions": {
                            "table": table_name,
                            "escapeCharacter": "5C", # ASCII hexadecimal for backslash
                            "quoteCharacter": "22",  # ASCII hexadecimal for double quote
                            "fieldDelimiter": "2C"   # ASCII hexadecimal for comma
                        },
                        "api_key": 'AIzaSyAlv29u_dHxLECJUBj9aQJx5vwndfEx9fs'
                    }
                }
                
                while True:
                    try:
                        request = service.instances().import_(
                            project=project_id,
                            instance=instance_id,
                            body=import_request_body
                        )

                        response = request.execute()

                        print(response)
        
                        break
                    except errors.HttpError as e:
                        if e.resp.status == 409:  # If the error is 'operationInProgress'
                            print("Operation in progress, waiting...")
                            time.sleep(10)  # Wait for 10 seconds before trying again
                        else:
                            raise  # If the error is something else, raise it
            # blob.delete()
        except Exception as e:
            pass
        
        
        # credentials_location = r'scripts\cloudsql\credentials.json'
        # os.system(f'gcloud auth activate-service-account --key-file={credentials_location}')
        # try:
        #     project_id = 'library-414223'
        #     instance_id = 'library'
        #     database = 'library'
        #     bucket_name = 'data_warehousing_library_data'
        #     storage_client = storage.Client.from_service_account_json(credentials)
        #     bucket = storage_client.get_bucket(bucket_name)

        #     for file in files:
        #         filename = file.split('\\')[-1]
        #         blob = bucket.blob(filename)
        #         with open(file, 'rb') as f:
        #             blob.upload_from_file(f)

        #         table_name = filename.split('.')[0]

        #         file_uri = f'gs://{bucket_name}/{filename}'

        #         os.system(f' echo y | gcloud sql import csv library {file_uri}'
        #                     ' --project=library-414223'
        #                     ' --database=library'
        #                     f' --table={table_name}'
        #                     ' --quote="22"'
        #                     ' --escape="5C"'
        #                     ' --fields-terminated-by="2C"'
        #                 )

        #         blob.delete()
        # except Exception as e:
        #     print(e)
        #     return