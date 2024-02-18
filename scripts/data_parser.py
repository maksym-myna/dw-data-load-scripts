import parsers.ol_readings_parser as olreadsp
import parsers.ol_ratings_parser as olratesp
import parsers.ol_dump_parser as oldumpp
import parsers.sl_dump_parser as sldumpp
import concurrent.futures
import gdrive.auth as gauth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

auth = gauth.GDriveAuth()

ol_parsers = [
    oldumpp.OLDumpParser(),
    olreadsp.OLReadingsParser(),
    olratesp.OLRatingsParser()
]
sl_parser = sldumpp.SLDataParser()

directory = r'..\open library dump'

with concurrent.futures.ThreadPoolExecutor() as executor:
    list_of_file_lists = {executor.submit(parser.process_latest_file, directory) for parser in ol_parsers}
    list_of_file_lists.add(executor.submit(sl_parser.process_file, r'..\seattle library dump\checkouts.json', r'..\seattle library dump\data\seattle_library.json'))
    
    for future in concurrent.futures.as_completed(list_of_file_lists):
        try:
            service = build("drive", "v3", credentials=auth.creds)
            files = future.result()
            for file_location in files:
                file_name = file_location.split('\\')[-1]

                search = service.files().list(q=f"name='{file_name}'").execute()
                
                media_body = MediaFileUpload(file_location, mimetype='application/json', resumable=True)

                file = search.get('files')[0]        
                id=file.get('id')
                    
                updated_file = service.files().update(
                    fileId=id,
                    body={},
                    media_body=media_body
                ).execute()
            
                versions = service.revisions().list(fileId=id).execute()
            
                service.revisions().delete(fileId=id, revisionId=versions['revisions'][0]['id']).execute()
        except Exception as e:
            print(f"An error occurred: {e}")        

