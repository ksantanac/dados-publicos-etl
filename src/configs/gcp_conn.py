import logging
from google.cloud import storage, bigquery

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GCSClient:
    def __init__(self, project_id: str, bucket_name: str):
        self.project_name = project_id
        self.bucket_name = bucket_name
        self.storage_client = storage.Client(project=str(project_id))
        self.bigquery_client = bigquery.Client(project=str(project_id))
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_to_gcs(self, destination_blob_name, file_content):
        logging.info(f"Iniciando o envio do arquivo {destination_blob_name} para o bucket {self.bucket_name}.")

        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(file_content, content_type="application/octet-stream")

        logging.info(f"Arquivo {destination_blob_name} enviado para o bucket {self.bucket_name}.")

    def list_blobs(self, folder_path: str):
        """
        Lista todos os blobs (arquivos) na pasta especificada.
        :param folder_path: Caminho da pasta no bucket do GCS.
        :return: Lista de blobs.
        """
        return self.bucket.list_blobs(prefix=folder_path)