import abc
import os
import io
from abc import ABC
from dotenv import load_dotenv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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
    def push_new_version(self) -> bool:
        """
        Tag and push a new version of the ETL process to the loading platform
        :return:
        """
        pass


class Drive(Client):
    def __init__(self):
        scope = [os.getenv('SCOPE')]
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', scope)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    '../credentials.json', scope)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        self.service = build(os.getenv('API_NAME'), os.getenv('API_VERSION'), credentials=creds)

        # Init temporal space for static data
        if not os.path.exists(os.getenv('TEMPORAL_STATIC_DATA_PATH')):
            os.makedirs(os.getenv('TEMPORAL_STATIC_DATA_PATH'))

    def retrieve_static_data(self) -> list:
        results = (self.service.files().list(
            q=f"name = '{os.getenv('WAREHOUSE_FOLDER')}'",
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

    def push_new_version(self) -> bool:
        pass

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
