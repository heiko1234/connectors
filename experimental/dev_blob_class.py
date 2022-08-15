



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

# load_dotenv()

# local_run = os.getenv("LOCAL_RUN", False)




local_run = True
container_name = "coinbasedata"



# class BlobStorageConnector:
#     def __init__(self, container_name):
#         self.__container = container_name

#         # self._connection_string = None
    
#         connection_string_str = None


#         try:
#             connection_string_str  = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
#             account_name = os.getenv("AZURE_STORAGE_NAME")
#             account_key = os.getenv("AZURE_STOREAGE_KEY")
#         except BaseException:
#             connection_string_str  = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
#             account_name = os.environ["AZURE_STORAGE_NAME"]
#             account_key = os.environ("AZURE_STOREAGE_KEY")

#         if connection_string_str is not None:
#             self._connection_string = connection_string_str 
        
#         else:
#             self._connection_string = ";".join(
#                 [
#                     "DefaultEndpointsProtocol=http",
#                     f"AccountName={account_name}",
#                     f"AccountKey={account_key}",
#                     f"DefaultEndpointsProtocol=http",
#                     f"BlobEndpoint=http://127.0.0.1:10000/{account_name}",
#                     f"QueueEndpoint=http://127.0.0.1:10001/{account_name}",
#                 ]
#             )

#     def get_container_client(self):
#         blob_container_client = ContainerClient.from_connection_string(
#             self._connection_string, container_name=self.__container
#         )
#         return blob_container_client

#     def list_files_in_subcontainer(self, subcontainer, files_with):
#         output = []
#         for blob in self.get_container_client().list_blobs():
#             if subcontainer in blob.name and files_with in blob.name:
#                 output.append(blob.name.split("/")[1])
#         return output





if local_run:
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
else:
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

connection_string
container_name = "coinbasedata"



ContainerClient.from_connection_string(connection_string, container_name=container_name)



blob_container_client = ContainerClient.from_connection_string(
    conn_str=connection_string, container_name=container_name
)
print("client by connection string")





blob_container_client.list_blobs()


container_name = "coinbasedata"

subcontainer = "coinbasedata"
files_with = ".parquet"



container_name = "sklearn"
subcontainer = "data"
files_with = ".parquet"




output = []
for blob in ContainerClient.from_connection_string(connection_string, container_name).list_blobs():
    print(blob.name)
    if subcontainer in blob.name and files_with in blob.name:
        output.append(blob.name.split("/")[1])





# blobclient

subcontainer="coinbasedata"
file="coinbase_data.parquet"

# blob_str = subcontainer + "/" + file
# blob_str
# bytes = blobclient.get_blob_client(blob=blob_str).download_blob().readall()
# any_file = io.BytesIO(bytes)


# df = pd.read_parquet(any_file, engine="pyarrow")

# df



storage_account_name="myconnection"

abfs = AzureBlobFileSystem(
    account_name=storage_account_name,
    anon=False,
    credentail=AioAzureCliCredential()
    )
file_system = fs.PyFileSystem(fs.FSSpecHandler(abfs))

filepath = f"/{subcontainer}/{file}"

df = pq.read_table(
    source=container_name + filepath, columns = None, filesystem=file_system
    ).to_pandas()

df

