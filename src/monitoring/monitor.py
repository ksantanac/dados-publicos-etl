from google.cloud import storage, bigquery
import pandas as pd
import io, os, sys
import logging
import uuid
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(src_dir)

from configs.gcp_conn import GCSClient

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

class BigQueryMonitoring:

    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.gcs_client = GCSClient(project_id=project_id, bucket_name=bucket_name)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized BigQueryMonitoring for project {project_id} and bucket {bucket_name}")

    def generate_uid(self):
        """Gera um identificador único universal (UUID)"""
        uid = str(uuid.uuid4())
        self.logger.debug(f"Generated UID: {uid}")
        return uid

    def count_rows_in_file(self, bucket_name, file_path, delimiter):
        """Conta o número de linhas em um arquivo .csv ou .parquet"""
        try:
            self.logger.info(f"Counting rows in file: gs://{bucket_name}/{file_path}")
            client = self.gcs_client.storage_client
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            if file_path.endswith('.csv'):
                self.logger.debug("Processing CSV file")
                content = blob.download_as_string().decode('utf-8')
                df = pd.read_csv(io.StringIO(content), sep=delimiter)
                row_count = len(df)
                self.logger.info(f"CSV file has {row_count} rows")
                return row_count
                
            elif file_path.endswith('.parquet'):
                self.logger.debug("Processing Parquet file")
                content = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(content))
                row_count = len(df)
                self.logger.info(f"Parquet file has {row_count} rows")
                return row_count
            else:
                error_msg = f"Unsupported file format: {file_path}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
                
        except Exception as e:
            self.logger.error(f"Error counting rows in file {file_path}: {str(e)}")
            raise

    def get_total_rows(self, bucket_name, folder_path, delimiter):
        """Calcula o total de linhas em todos os arquivos de uma pasta"""
        try:
            self.logger.info(f"Calculating total rows in folder: gs://{bucket_name}/{folder_path}")
            client = self.gcs_client.storage_client
            bucket = client.get_bucket(bucket_name)
            
            if not folder_path.endswith('/'):
                folder_path += '/'
            
            blobs = bucket.list_blobs(prefix=folder_path)
            total_rows = 0
            processed_files = 0
            
            for blob in blobs:
                if not blob.name.endswith('/'):
                    try:
                        self.logger.debug(f"Processing file: {blob.name}")
                        file_rows = self.count_rows_in_file(bucket_name, blob.name, delimiter)
                        total_rows += file_rows
                        processed_files += 1
                    except ValueError as e:
                        self.logger.warning(f"Skipping file {blob.name}: {str(e)}")
                        continue
            
            self.logger.info(f"Processed {processed_files} files with total of {total_rows} rows")
            return total_rows
            
        except Exception as e:
            self.logger.error(f"Error getting total rows: {str(e)}")
            raise

    def count_files_in_folder(self, bucket_name, folder_path):
        """Conta quantos arquivos existem em uma pasta específica do GCS"""
        try:
            self.logger.info(f"Counting files in folder: gs://{bucket_name}/{folder_path}")
            client = self.gcs_client.storage_client
            bucket = client.get_bucket(bucket_name)
            
            if not folder_path.endswith('/'):
                folder_path += '/'
            
            blobs = bucket.list_blobs(prefix=folder_path)
            files = [blob for blob in blobs if not blob.name.endswith('/')]
            file_count = len(files)
            
            self.logger.info(f"Found {file_count} files in folder")
            return file_count
            
        except Exception as e:
            self.logger.error(f"Error counting files: {str(e)}")
            raise

    def insert_monitoring_data(self, dataset_id, table_id, nm_project, step, qtd_files, qtd_rows, dt_start=None, dt_end=None):
        """Insere dados de monitoramento na tabela do BigQuery"""
        try:
            self.logger.info(f"Inserting monitoring data into {dataset_id}.{table_id}")
            
            if dt_start is None:
                dt_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if dt_end is None:
                dt_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Gera um UID para o registro
            uid = self.generate_uid()
            self.logger.debug(f"Generated UID for this record: {uid}")
            
            self.logger.debug(f"Data to insert: ID={uid}, project={nm_project}, step={step}, files={qtd_files}, rows={qtd_rows}")
            
            client = self.gcs_client.bigquery_client
            table_ref = client.dataset(dataset_id).table(table_id)
            
            rows_to_insert = [{
                "ID_MONITORING": uid,
                "NM_PROJECT": nm_project,
                "STEP": step,
                "QTD_FILES": int(qtd_files),
                "QTD_ROWS": int(qtd_rows),
                "DT_START": dt_start,
                "DT_END": dt_end
            }]
            
            errors = client.insert_rows_json(table_ref, rows_to_insert)
            
            if errors:
                error_msg = f"Errors inserting data: {errors}"
                self.logger.error(error_msg)
                return False
            else:
                self.logger.info("Data inserted successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error inserting monitoring data: {str(e)}")
            raise

    def run(self, dataset_id, table_id, bucket_name, folder_path, nm_project, step, delimiter):
        """Função principal que orquestra o monitoramento"""
        try:
            self.logger.info(f"Starting monitoring process for project {nm_project}, step {step}")
            
            dt_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.debug(f"Process start time: {dt_start}")
            
            self.logger.info("Counting files...")
            qtd_files = self.count_files_in_folder(bucket_name, folder_path)
            
            self.logger.info("Counting rows...")
            qtd_rows = self.get_total_rows(bucket_name, folder_path, delimiter)
            
            dt_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.debug(f"Process end time: {dt_end}")
            
            self.logger.info(f"Metrics collected - Files: {qtd_files}, Rows: {qtd_rows}")
            
            success = self.insert_monitoring_data(
                dataset_id=dataset_id,
                table_id=table_id,
                nm_project=nm_project,
                step=step,
                qtd_files=qtd_files,
                qtd_rows=qtd_rows,
                dt_start=dt_start,
                dt_end=dt_end
            )
            
            if success:
                self.logger.info("Monitoring process completed successfully")
            else:
                self.logger.warning("Monitoring process completed with warnings")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in monitoring process: {str(e)}")
            raise