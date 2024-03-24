import db_secrets as secrets

import time
from parsers.data_processor import DataProcessor
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient import discovery, errors
from google.cloud.sql.connector import Connector
import sqlalchemy


class CSVDataprocessor(DataProcessor):
    def __init__(self):
        super().__init__("csv")

        self.CREDENTIALS = r"scripts\cloudsql\credentials.json"

        self.INSTANCE_CONNECTION_NAME = (
            f"{secrets.PROJECT_ID}:{secrets.REGION}:{secrets.INSTANCE_ID}"
        )

    def run(
        self, old_directory=r"open library dump", sld_directory=r"seattle library dump"
    ) -> None:
        self.download_and_unarchive_datasets()

        files = self.old_parser.process_latest_file(old_directory)
        files.extend(
            [
                parser.process_latest_file(old_directory, self.old_parser.work_ids)
                for parser in self.ol_parsers
            ]
        )
        files.extend(
            self.sl_parser.process_file(
                rf"{sld_directory}\checkouts.json",
                [
                    rf"{sld_directory}\data\loan.{self.type_name}",
                    rf"{sld_directory}\data\loan_return.{self.type_name}",
                ],
            )
        )

        self.sqlite_conn.close()
        self.user_manager.writeUsers()

        service_credentials = service_account.Credentials.from_service_account_file(
            self.CREDENTIALS
        )
        service = discovery.build(
            "sqladmin", "v1beta4", credentials=service_credentials
        )

        sql_connector = Connector(credentials=service_credentials)

        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://", creator=lambda: self.create_conn(sql_connector)
        )
        with pool.connect() as db_conn:
            db_conn.execute(
                sqlalchemy.text(
                    open(
                        "scripts/sql/database_schema.sql", "r", encoding="utf-8"
                    ).read()
                )
            )
            db_conn.commit()
            db_conn.close()

        storage_client = storage.Client.from_service_account_json(self.CREDENTIALS)
        bucket = storage_client.get_bucket(secrets.BUCKET_NAME)
        try:
            for file in files:
                filename = file.split("/")[-1]
                blob = bucket.blob(filename)

                with open(file, "rb") as f:
                    blob.upload_from_file(f)
                table_name = filename.split(".")[0]

                file_uri = f"gs://{secrets.BUCKET_NAME}/{filename}"

                import_request_body = {
                    "importContext": {
                        "fileType": "CSV",
                        "uri": file_uri,
                        "database": secrets.DB_NAME,
                        "csvImportOptions": {
                            "table": table_name,
                            "escapeCharacter": "5C",  # ASCII hexadecimal for backslash
                            "quoteCharacter": "22",  # ASCII hexadecimal for double quote
                            "fieldDelimiter": "2C",  # ASCII hexadecimal for comma
                        },
                        "api_key": secrets.CLOUD_SQL_API_KEY,
                    }
                }

                while True:
                    try:
                        request = service.instances().import_(
                            project=secrets.PROJECT_ID,
                            instance=secrets.INSTANCE_ID,
                            body=import_request_body,
                        )

                        response = request.execute()

                        print(
                            f'importing from {response.get("importContext").get("uri") } to the table {response.get("importContext").get("csvImportOptions").get("table")} is {response.get("status")}'
                        )
                        break
                    except errors.HttpError as e:
                        if (e.resp.status == 409):  # If the error is 'operationInProgress'
                            print("Operation in progress, waiting...")
                            time.sleep(10)
                        else:
                            continue
        except Exception:
            pass

    def create_conn(
        self, sql_connector: Connector
    ) -> sqlalchemy.engine.base.Connection:
        """
        Establishes a connection to the database.

        Args:
            sql_connector: The SQL connector object used to establish the connection.

        Returns:
            A connection object to the database.
        """
        return sql_connector.connect(
            self.INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=secrets.DB_USER,
            password=secrets.DB_PASS,
            db=secrets.DB_NAME,
        )
