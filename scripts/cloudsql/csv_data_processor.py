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
                                        [rf'{sld_directory}\data\loan.csv', rf'{sld_directory}\data\return.csv']))
        credentials = r'scripts\cloudsql\credentials.json'
        os.system(f'gcloud auth activate-service-account --key-file={credentials}')

        old = r'open library dump'
        sld = r'seattle library dump'
    
        files = [
            # rf'{old}\data\language.csv',
            rf'{old}\data\author.csv',
            rf'{old}\data\edition.csv',
            rf'{old}\data\edition_language.csv',
            rf'{old}\data\edition_publisher.csv',
            rf'{old}\data\isbn_10.csv',
            rf'{old}\data\isbn_13.csv',
            rf'{old}\data\edition_work.csv',
            rf'{old}\data\edition_series.csv',
            rf'{old}\data\work.csv',
            rf'{old}\data\subject.csv',
            rf'{old}\data\work_author.csv',
            
            rf'{old}\data\user.csv',

            rf'{old}\data\rating.csv',
            rf'{old}\data\listing.csv',
            rf'{old}\data\inventory_lot.csv',
            rf'{old}\data\author.csv',
            rf'{sld}\data\loan.csv',
            rf'{sld}\data\return.csv'
        ]
        
        # files =[]
        # for future in concurrent.futures.as_completed(list_of_file_lists):
        #     files = future.result() + files
        try:
            bucket_name = 'data_warehousing_library_data'
            storage_client = storage.Client.from_service_account_json(credentials)
            bucket = storage_client.get_bucket(bucket_name)
            
            for file in files:
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
        except Exception as e:
            print(e)
            return