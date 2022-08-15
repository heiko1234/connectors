
from blob.blob_connector import BlobStorageConnector


coinbase_datafetcher = BlobStorageConnector(container_name="coinbasedata", local_run=True)



data = coinbase_datafetcher.get_parquet(subcontainer="coinbasedata", file="coinbase_data.parquet")
data


data = coinbase_datafetcher.get_part_parquet(subcontainer="coinbasedata", file="coinbase_data.parquet")
data




