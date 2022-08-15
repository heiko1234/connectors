
# Dagster and MLFlow

This Repo is about to use MLFlow within a Dagster Pipeline


This Repo is based on Poetry and can be executed within an virtual environment.

## Install

This Repo is based on poetry

```bash

python3 -m venv .venv

# or
python -m venv .venv

# switch manually to virtual environment and then

$(.venv) pip install poetry

$(.venv) poetry install 
# will install all dependencies from the pyproject.toml file

```

## Create an .env file


I use for local development Azurite and included the connectionsting of azurite to the .env file.

Moreover, the Blobcontainter name for the MLFlow Models: "model-container" and the basics for MLFlow Tracking are added.

```bash

LOCAL_RUN=True

# Working with Azurite
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;QueueEndpoint=http://localhost:10001/devstoreaccount1"

AZURE_STORAGE_ACCOUNT="devstoreaccount1"

AZURE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


BLOB_MODEL_CONTAINER_NAME="model-container"


MLFLOW_TRACKING_URI="http://localhost:5000"
MLFLOW_MODEL_DIRECTORY = "models:"
MLFLOW_MODEL_STAGE = "Staging"


```


## Repo Structure

This Repo contains of several major folders.

```bash 

|
|- blob
|- kusto 
|- mlflow
|- experimental


```

The .yaml File in "config_for_azure_blobstorage" should be added the azurite container of this project.

