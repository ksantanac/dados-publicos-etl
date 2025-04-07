from google.cloud import storage
import pandas as pd
import logging
import sys, os
from io import StringIO, BytesIO

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(src_dir)

from configs.gcp_conn import GCSClient

# Função para converter arquivo para parquet
def file_to_parquet(bucket_name: str, project_id: str, trusted_folder: str, refined_folder: str):
    """
    Converte todos os CSVs da pasta trusted para Parquet na pasta refined
    garantindo o caminho correto no GCS.
    
    Args:
        bucket_name (str): Nome do bucket GCS
        project_id (str): ID do projeto GCP
        trusted_folder (str): Pasta de origem (ex: 'trusted/cnaes')
        refined_folder (str): Pasta de destino (ex: 'refined/cnaes')
    """
    try:
        # Inicializar o GCSClient
        gcs_client = GCSClient(project_id=project_id, bucket_name=bucket_name)
        
        # Garante que as pastas terminam com '/'
        trusted_folder = trusted_folder.rstrip('/') + '/'
        refined_folder = refined_folder.rstrip('/') + '/'
        
        # Lista todos os CSVs na pasta trusted usando o método da classe
        blobs = gcs_client.list_blobs(trusted_folder)
        csv_files = [b for b in blobs if b.name.endswith('.csv')]
        
        if not csv_files:
            logging.warning(f"Nenhum CSV encontrado em gs://{bucket_name}/{trusted_folder}")
            return

        for csv_blob in csv_files:
            try:
                # Extrai o nome base do arquivo
                base_name = csv_blob.name[len(trusted_folder):]
                parquet_name = base_name.replace('.csv', '.parquet')
                parquet_path = f"{refined_folder}{parquet_name}"
                
                # Download e conversão
                csv_data = csv_blob.download_as_text(encoding='utf-8')
                df = pd.read_csv(StringIO(csv_data), delimiter='|')
                
                # Upload do Parquet usando o método da classe GCSClient
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                gcs_client.upload_to_gcs(
                    destination_blob_name=parquet_path,
                    file_content=parquet_buffer.getvalue()
                )
                
                logging.info(f"Arquivo convertido e salvo em: gs://{bucket_name}/{parquet_path}")

            except Exception as e:
                logging.error(f"Erro ao converter {csv_blob.name}: {str(e)}")
                continue

        logging.info("Conversão concluída com sucesso!")

    except Exception as e:
        logging.error(f"Falha geral: {str(e)}")
        raise


# convert_trusted_to_parquet(
#     bucket_name="big-data-dw",
#     project_id="api-spring-bot",
#     trusted_folder="trusted",
#     refined_folder="refined"
# )