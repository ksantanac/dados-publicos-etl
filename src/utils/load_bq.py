import sys, os

from google.cloud import bigquery, storage


current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(src_dir)

from .scraping import logging
from configs.gcp_conn import GCSClient

def load_parquets_to_bigquery(bucket_name: str, project_id: str, refined_folder_path: str, dataset_id: str, table_id: str):
    """
    Carrega todos os arquivos Parquet de uma pasta para uma tabela no BigQuery,
    utilizando totalmente a classe GCSClient.
    
    Args:
        bucket_name (str): Nome do bucket GCS
        project_id (str): ID do projeto GCP
        refined_folder_path (str): Caminho da pasta refined no bucket
        dataset_id (str): ID do dataset no BigQuery
        table_id (str): ID da tabela no BigQuery
    """
    try:
        # Inicializar o GCSClient (que já contém ambos os clients)
        gcs_client = GCSClient(project_id=project_id, bucket_name=bucket_name)
        
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        logging.info(f"Iniciando carga de Parquets para {full_table_id}")
        
        # Listar arquivos Parquet usando o método da classe GCSClient
        blobs = gcs_client.list_blobs(refined_folder_path)
        parquet_files = [f"gs://{bucket_name}/{b.name}" for b in blobs if b.name.endswith('.parquet')]
        
        if not parquet_files:
            logging.warning("Nenhum arquivo Parquet encontrado na pasta refined")
            return
        
        logging.info(f"Encontrados {len(parquet_files)} arquivos Parquet")
        
        # Configuração do job de carga (usando o client BigQuery da classe)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        # Carregar arquivos usando o client BigQuery da classe
        load_job = gcs_client.bigquery_client.load_table_from_uri(
            source_uris=parquet_files,
            destination=full_table_id,
            job_config=job_config
        )
        
        load_job.result()
        
        # Verificar resultados
        destination_table = gcs_client.bigquery_client.get_table(full_table_id)
        logging.info(f"Dados carregados com sucesso! Total de linhas: {destination_table.num_rows}")
        
    except Exception as e:
        logging.error(f"Erro ao carregar Parquets para BigQuery: {str(e)}")
        raise


# # 2. Depois carregue para o BigQuery
# load_parquets_to_bigquery(
#     bucket_name="big-data-dw",
#     project_id="api-spring-bot",
#     refined_folder_path="refined",
#     dataset_id="big_data_projects",
#     table_id="cnaes"
# )