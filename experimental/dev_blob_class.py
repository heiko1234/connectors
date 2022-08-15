



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




if local_run:
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
else:
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

connection_string


blob_container_client = ContainerClient.from_connection_string(
    conn_str=connection_string, container_name=container_name
)
print("client by connection string")


blobclient = blob_container_client


blobclient.list_blobs()




blobclient

subcontainer="coinbasedata"
file="coinbase_data.parquet"

blob_str = subcontainer + "/" + file
blob_str
bytes = blobclient.get_blob_client(blob=blob_str).download_blob().readall()
any_file = io.BytesIO(bytes)


df = pd.read_parquet(any_file, engine="pyarrow")

df



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

