import abc
import os
import io
from abc import ABC
from typing import Any

from dotenv import load_dotenv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

load_dotenv()


class Client(ABC):
    @abc.abstractmethod
    def retrieve_static_data(self) -> list:
        """
        Retrieve the static data from the warehouse and return a list of files
        :return:
        """
        pass

    @abc.abstractmethod
    def push_file(self, path_to_file: str, save_into: str, filename_str) -> bool:
        """
        Tag and push a new version of the ETL process to the loading platform
        :return:
        """
        pass

    @abc.abstractmethod
    def get_resource(self, resource_id: str, resource_name: str):
        pass


class Drive(Client):
    def __init__(self, credentials: Credentials) -> None:
        scope = [os.getenv('SCOPE')]
        creds = credentials
        creds.refresh(Request())
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', scope)
        self.service = build(os.getenv('API_NAME'), os.getenv('API_VERSION'), credentials=creds)

        # Init temporal space for static data
        if not os.path.exists(os.getenv('TEMPORAL_STATIC_DATA_PATH')):
            os.makedirs(os.getenv('TEMPORAL_STATIC_DATA_PATH'))

    def retrieve_static_data(self) -> list:
        results = (self.service.files().list(
            q=f"name = '{os.getenv('STORAGE_NAME')}'",
            fields="nextPageToken, files(id, name)")
                   .execute())
        items = results.get('files', [])
        if not items:
            return []

        folder_id = items[0]['id']
        results = self.service.files().list(q=f"'{folder_id}' in parents",
                                            fields="files(id, name)").execute()
        for resource in results.get('files'):
            if resource.get('name') == os.getenv('STATIC_SOURCE_FOLDER'):
                folder_id = resource.get('id')
                results = self.service.files().list(q=f"'{folder_id}' in parents",
                                                    fields="files(id, name)").execute()
                items = results.get('files')
        return items

    def get_resource(self, resource_id: str, resource_name: str):
        """
        Download the resource to the local storage to be analyzed
        :param resource_name:
        :param resource_id:
        :return:
        """
        request = self.service.files().get_media(fileId=resource_id)
        fh = io.FileIO(f'{os.getenv("TEMPORAL_STATIC_DATA_PATH")}{resource_name}', 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

    def push_file(self, path: str, parent_id: str, filename: str) -> None:
        file_metadata = {'name': filename, "parents": [parent_id]}
        media = MediaFileUpload(path, mimetype="application/parquet")
        self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()

    def serialize(self) -> dict[str, Any]:
        return {
            "service": self.service,
        }
