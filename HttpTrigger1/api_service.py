import logging
import requests
import csv
import os, json
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobLeaseClient, BlobPrefix, ContentSettings
from datetime import datetime

def get_data():
    data_url = os.getenv("DataUrl") 
    logging.info(f"Loading data from url:{data_url}")    
    print(data_url)
    req = requests.get(f"{data_url}")
    data = req.content.decode("utf-8")
    return data

def upload_blob_data(data):
    blobname = 'station-' + datetime.now().strftime("%Y-%m-%d--%H:%M:%S")
    container_name = os.getenv("InputContainer") 
    
    keyVaultName = os.getenv("keyVaultName")
    secret_client = SecretClient(vault_url=f"https://{keyVaultName}.vault.azure.net", credential=DefaultAzureCredential())

    retrieved_secret = secret_client.get_secret(os.getenv("SecretNameInputContainerConnString"))
    connection_string = retrieved_secret.value
    logging.info(f"connection_string:{connection_string}")   

    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blobname}.csv")
    data = data.encode('utf8')

    blob_client.upload_blob(data, blob_type="BlockBlob")
    logging.info(f"Finish upload data to blob")

