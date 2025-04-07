import logging
import requests
from bs4 import BeautifulSoup
import zipfile
import io
import codecs

import os, sys

from google.api_core.exceptions import GoogleAPIError

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(src_dir)

from configs.gcp_conn import GCSClient


# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para download
def download_file(project_id: str, bucket_name: str):
    url_base = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/"
    
    try:
        # Inicializar cliente GCS
        gcs_client = GCSClient(project_id, bucket_name)
        
        # Fazer requisição para a página
        logging.info("Acessando a página...")
        response = requests.get(url_base)
        response.raise_for_status()
        
        # Parsear o conteúdo HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Procurar o link para Cnaes.zip
        link_arquivo = None
        for link in soup.find_all('a'):
            if link.get('href') == "Cnaes.zip":
                link_arquivo = url_base + link.get('href')
                break
        
        if not link_arquivo:
            logging.info("Arquivo Cnaes.zip não encontrado na página.")
            return False
        
        logging.info(f"Arquivo encontrado: {link_arquivo}")
        
        # Baixar o arquivo ZIP em memória
        logging.info("Iniciando download do arquivo ZIP...")
        response_arquivo = requests.get(link_arquivo, stream=True)
        response_arquivo.raise_for_status()
        
        # Criar um objeto BytesIO para armazenar o arquivo ZIP em memória
        zip_in_memory = io.BytesIO()
        for chunk in response_arquivo.iter_content(chunk_size=8192):
            zip_in_memory.write(chunk)
        
        logging.info("Download do ZIP concluído. Processando arquivo...")
        
        # Extrair o arquivo ZIP em memória
        with zipfile.ZipFile(zip_in_memory) as zip_ref:
            # Listar arquivos dentro do ZIP
            logging.info("Arquivos dentro do ZIP:")
            for file in zip_ref.namelist():
                logging.info(f"- {file}")
            
            # Verificar se há arquivos
            if not zip_ref.namelist():
                logging.info("Nenhum arquivo encontrado dentro do ZIP.")
                return False
            
            # Pegar o primeiro arquivo (assumindo que é o que queremos)
            original_file_name = zip_ref.namelist()[0]
            novo_nome = "Cnaes.csv"
            
            # Ler o conteúdo do arquivo com a codificação correta
            with zip_ref.open(original_file_name) as file:
                conteudo = file.read().decode('iso-8859-1')
                
            # Definir o caminho de destino no GCS com o novo nome
            destination_path = f"raw/{novo_nome}"
            
            # Fazer upload do conteúdo para o GCS
            gcs_client.upload_to_gcs(destination_path, conteudo.encode('utf-8'))
            
            logging.info(f"Arquivo renomeado para {novo_nome} e enviado para o GCS no caminho: {destination_path}")
        
        return True
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro de conexão: {e}")
        return False
    except zipfile.BadZipFile:
        logging.error("Erro: O arquivo ZIP está corrompido ou inválido.")
        return False
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
        return False
    
# Add BOM CSV
def add_bom_csv(bucket_name: str, project_id: str, folder_path: str):
    """
    Procura o arquivo 'part-*.csv' na pasta do GCS, adiciona BOM e sobrescreve o arquivo original.
    Utiliza a classe GCSClient para as operações com o Google Cloud Storage.
    
    Args:
        bucket_name (str): Nome do bucket GCS
        project_id (str): ID do projeto GCP
        folder_path (str): Caminho da pasta no bucket para procurar o arquivo CSV
    
    Raises:
        GoogleAPIError: Erros relacionados à API do Google Cloud
        Exception: Outros erros inesperados
    """
    try:
        # Inicializa o cliente GCS
        gcs_client = GCSClient(project_id=project_id, bucket_name=bucket_name)
        
        logging.info(f"🔍 Procurando arquivo CSV em: gs://{bucket_name}/{folder_path}")
        
        # Listar arquivos da pasta
        blobs = list(gcs_client.list_blobs(folder_path))

        # Encontrar o part-*.csv
        target_blob = None
        for blob in blobs:
            if blob.name.endswith(".csv") and "part-" in blob.name:
                target_blob = blob
                break

        if not target_blob:
            logging.warning("❌ Nenhum arquivo CSV encontrado com o padrão 'part-*.csv'.")
            return

        logging.info(f"🎯 Arquivo encontrado: {target_blob.name}")

        try:
            # Baixar conteúdo como bytes diretamente na memória
            content_bytes = target_blob.download_as_bytes()
            logging.info(f"📥 Arquivo baixado ({len(content_bytes)} bytes)")

            # Verificar se já tem BOM
            if content_bytes.startswith(codecs.BOM_UTF8):
                logging.info("ℹ️ O arquivo já contém BOM UTF-8. Nenhuma modificação necessária.")
                return

            # Adicionar BOM no início
            content_with_bom = codecs.BOM_UTF8 + content_bytes

            # Fazer upload para o mesmo local, substituindo o arquivo original
            logging.info(f"⬆️ Preparando upload para substituir o arquivo original")
            
            # Fazer upload para o mesmo caminho usando o GCSClient
            gcs_client.upload_to_gcs(
                destination_blob_name=target_blob.name,  # Mesmo caminho original
                file_content=content_with_bom
            )

            logging.info(f"✅ Arquivo processado com sucesso! BOM adicionado e arquivo substituído em: gs://{bucket_name}/{target_blob.name}")

        except GoogleAPIError as e:
            logging.error(f"🚨 Erro ao processar o arquivo {target_blob.name}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"🚨 Erro inesperado ao processar o arquivo: {str(e)}")
            raise

    except GoogleAPIError as e:
        logging.error(f"🚨 Erro de conexão com o Google Cloud Storage: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"🚨 Erro inesperado: {str(e)}")
        raise

# if __name__ == "__main__":
#     # Configurações do projeto e bucket
#     PROJECT_NAME = "api-spring-bot"  # Substitua pelo seu projeto GCP
#     BUCKET_NAME = "big-data-dw"
    
#     baixar_e_extrair_arquivo_cnpj(PROJECT_NAME, BUCKET_NAME)