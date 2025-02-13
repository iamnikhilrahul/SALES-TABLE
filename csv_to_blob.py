from azure.storage.blob import BlobServiceClient
import os
CONTAINER_NAME = "source"
connection_string = f"**************************************************************************************"

PATH = "C:/AZURE/SALES_DATA/"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)\

csv_files = [f for f in os.listdir(PATH) if f.endswith(".csv")]

for file_name in csv_files:
    FILE_PATH = os.path.join(PATH,file_name)
    blob_client = container_client.get_blob_client(file_name)

    with open(FILE_PATH, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"{file_name} uploaded successfully!")
