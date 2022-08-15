import os
import io

from pathlib import Path
import pandas as pd
import numpy as np

import pyarrow.fs as fs
import pyarrow.parquet as pq
import yaml
from adlfs import AzureBlobFileSystem

from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
    ServiceRequestError
)

from azure.identity import AzureCliCredential
from azure.identity.aio import AzureCliCredential as AioAzureCliCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from dotenv import load_dotenv





class BlobStorageConnector:
    def __init__(self, container_name: str, storage_account_name="mystorage", local_run=False):

        self.storage_account_name = storage_account_name
        self.url = f"https://{storage_account_name}.blob.core.windows.net"
        self.container_name = container_name
    
        try: 
            self.blobclient = self.get_client_by_string(container_name, local_run)
            # next(self.blobclient.list_blobs())
        except (ServiceRequestError):
            pass
        except (ResourceNotFoundError, ValueError, ClientAuthenticationError, KeyError, AttributeError):
            self.blobclient = self.get_client_by_cli(container_name)

    def get_client_by_string(self, container_name: str, local_run) -> ContainerClient:
        if local_run:
            load_dotenv()
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        else:
            connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_container_client = ContainerClient.from_connection_string(
            conn_str=connection_string, container_name=container_name
        )
        print("client by connection string")
        
        return blob_container_client

    def get_credentail(self):
        credentail = AzureCliCredential()
        return credentail
    
    def get_client_by_cli(self, container_name: str) -> ContainerClient:
        credential = self.get_credentail()
        blob_service_client = BlobServiceClient(self.url, credential=credential)
        blob_container_client = blob_service_client.get_container_client(container=container_name)
        print("client by cli")

        return blob_container_client

    def read_any(self, subcontainer, file):
        try:
            blob_str = subcontainer + "/" + file
            bytes = self.blobclient.get_blob_client(blob=blob_str).download_blob().readall()
            any_file = io.BytesIO(bytes)
            return any_file
        except Exception as e:
            print(e.message, e.args)
    
    def list_files_in_subcontainer(self, subcontainer, files_with):
        output = []
        for blob in self.blobclient.list_blobs():
            if subcontainer in blob.name and files_with in blob.name:
                output.append(blob.name.split("/")[1])
        return output
    
    def get_parquet(self, subcontainer, file) -> pd.DataFrame:
        pq_file = self.read_any(subcontainer=subcontainer, file = file)
        df = pd.read_parquet(pq_file, engine="pyarrow")
        return df
    
    def get_part_parquet(self, subcontainer, file) -> pd.DataFrame:
        abfs = AzureBlobFileSystem(
            account_name=self.storage_account_name,
            anon=False,
            credentail=AioAzureCliCredential()
        )
        file_system = fs.PyFileSystem(fs.FSSpecHandler(abfs))

        filepath = f"/{subcontainer}/{file}"

        df = pq.read_table(
            source=self.container_name + filepath, columns = None, filesystem=file_system
        ).to_pandas()
        return df

    def get_excel(self, subcontainer: str, file: str) -> pd.DataFrame:
        df = pd.read_excel(self.read_any(subcontainer=subcontainer, file=file, pyarrow=True), engine="openpyxl")
        return df

    def get_yaml(self, subcontainer: str, file: str):
        yaml_data = yaml.full_load(self.read_any(subcontainer=subcontainer, file=file))
        return yaml_data

    def upload_data_to_blob(
        self, df, subcontainer, filename, filetype="parquet"):

        relative_filename = f"./data/{subcontainer}/{filename}.{filetype}"
        file = Path(relative_filename)
        dir_to_create = file.parent
        os.makedirs(dir_to_create, exist_ok=True)

        if filetype == "parquet":
            df.to_parquet(relative_filename)
        if filetype== "csv":
            df.to_csv(relative_filename)

        print(f"Successfully intermediate save {relative_filename} locally")

        blob_str = f"{subcontainer}/{filename}.{filetype}"

        client = self.blobclient.get_container_client(blob=blob_str)

        with file.open("rb") as data:
            client.upload_blob(data, overwrite=True)
        print(f"successfully written {filename} in blob")

        # remove temporar file path
        Path(relative_filename).unlink()
        print(f"successfully deleted local file: {file}")





