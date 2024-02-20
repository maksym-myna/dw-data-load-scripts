import concurrent.futures
from data_processor import DataProcessor
from google.cloud import storage
import os

class CSVDataprocessor(DataProcessor):
    def __init__(self):
        super().__init__('csv')
    
    def run(self, old_directory = r'open library dump', sld_directory = r'seattle library dump') -> None:
        # cls.download_and_unarchive_datasets()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            
            list_of_file_lists = {executor.submit(parser.process_latest_file, old_directory)
                                for parser in self.ol_parsers}
            list_of_file_lists.add(executor.submit(self.sl_parser.process_file,
                                rf'{sld_directory}\checkouts.json',
                                        rf'{sld_directory}\data\checkout.csv'))

            os.system('gcloud auth activate-service-account --key-file=scripts\cloudsql\credentials.json')

        files = []
        for future in concurrent.futures.as_completed(list_of_file_lists):
            files = future.result() + files
        try:
            for file in files:
                credentials = r'scripts\cloudsql\credentials.json'
                bucket_name = 'data_warehousing_library_data'
            
                storage_client = storage.Client.from_service_account_json(credentials)
                bucket = storage_client.get_bucket(bucket_name)
                filename = file.split('\\')[-1]
                blob = bucket.blob(filename)
                with open(file, 'rb') as f:
                    blob.upload_from_file(f)
            
                table_name = filename.split('.')[0]
                
                file_uri = f'gs://{bucket_name}/{filename}'
        
                os.system(f' echo y | gcloud sql import csv library {file_uri}'
                            ' --project=library-414223'
                            ' --database=library'
                            f' --table={table_name}'
                            ' --quote="22"'
                            ' --escape="5C"'
                            ' --fields-terminated-by="2C"'
                        )

                blob.delete()
        except Exception:
            return