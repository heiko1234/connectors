
from blob.blob_connector import BlobStorageConnector


coinbase_datafetcher = BlobStorageConnector(container_name="coinbasedata", local_run=True)



data = coinbase_datafetcher.get_parquet(subcontainer="coinbasedata", file="coinbase_data.parquet")
data


data = coinbase_datafetcher.get_part_parquet(subcontainer="coinbasedata", file="coinbase_data.parquet")
data


config_list = coinbase_datafetcher.list_files_in_subcontainer(subcontainer="configuration_data", files_with=".yaml")
config_list

config=coinbase_datafetcher.get_yaml(subcontainer="configuration_data", file="job_mlflow_training_config.yaml")
config



# coinbase_datafetcher.upload_data_to_blob(df, subcontainer, filename, filetype="parquet")


# coinbase_datafetcher.get_excel(subcontainer, file)


