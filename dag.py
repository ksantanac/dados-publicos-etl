from datetime import datetime
from airflow import models
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator

from configparser import ConfigParser

import os, sys

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURR_DIR)
sys.path.append(CURR_DIR + "/src")

# Variavel definida no Airflow
ENV = Variable.get('ENV')

# ConfigParser
config = ConfigParser()
config.read(CURR_DIR + "/src/configs/dag-config.toml")

# GCP config
gcp_config = config[f'GCP-{ENV}']

# Variaveis
NM_PROJECT = gcp_config['gcp_nm_project']
PROJECT_ID = gcp_config['gcp_project_id']
REGION = gcp_config['gcp_region']
BUCKET_NAME = gcp_config['gcp_bucket_name']
DATASET_ID = gcp_config['gcp_dataset_id']
TABLE_ID = gcp_config['gcp_table_id']
TABLE_ID_MONITORING = gcp_config['gcp_table_id_monitoring']
CLUSTER_NAME = gcp_config['gcp_cluster_dataproc_name']
PYTHON_FILE = gcp_config['gcp_python_file_dataproc']

# Job do Spark
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYTHON_FILE},
}


@dag(
    dag_id="elt_cnaes",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataproc", "spark"],
)

def dag_etl_dados_publicos():

    from src.utils.scraping import download_file, add_bom_csv
    from src.utils.transform import file_to_parquet
    from src.utils.load_bq import load_parquets_to_bigquery

    from src.monitoring.monitor import BigQueryMonitoring

    monitoring = BigQueryMonitoring(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)

    # Task para download do arquivo
    scraping = PythonOperator(
        task_id="download_file",
        python_callable=download_file,
        op_kwargs={
            "project_id": PROJECT_ID,
            "bucket_name": BUCKET_NAME
        }
    )

    # Task para monitorar etapa RAW
    monitoring_raw = PythonOperator(
        task_id="monitoring_raw",
        python_callable= monitoring.run,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID_MONITORING,
            "bucket_name": BUCKET_NAME,
            "folder_path": "raw",
            "nm_project": NM_PROJECT,
            "step": "raw",
            "delimiter": ";"
        }
    )

    # Task para job spark
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )  

    # Task para add BOM ao arquivo
    add_bom = PythonOperator(
        task_id="add_bom",
        python_callable=add_bom_csv,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "folder_path": "trusted", 
        }
    )

    # Task para monitorar etapa trusted
    monitoring_trusted = PythonOperator(
        task_id="monitoring_trusted",
        python_callable= monitoring.run,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID_MONITORING,
            "bucket_name": BUCKET_NAME,
            "folder_path": "trusted",
            "nm_project": NM_PROJECT,
            "step": "trusted",
            "delimiter": "|"
        }
    )

    # Task para transforma arquivo para parquet
    csv_to_parquet = PythonOperator(
        task_id="csv_to_parquet",
        python_callable=file_to_parquet,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "trusted_folder": "trusted",
            "refined_folder": "refined"
        }
    )

    # Task para monitorar etapa refined
    monitoring_refined = PythonOperator(
        task_id="monitoring_refined",
        python_callable= monitoring.run,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID_MONITORING,
            "bucket_name": BUCKET_NAME,
            "folder_path": "refined",
            "nm_project": NM_PROJECT,
            "step": "refined",
            "delimiter": "|"
        }
    )

    # Task para carregar arquivo para BigQuery
    load_to_bq = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_parquets_to_bigquery,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "refined_folder_path": "refined",
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID
        }
    )


    # scraping >> submit_spark_job >> add_bom >> csv_to_parquet >> load_to_bq
    scraping >> monitoring_raw >> submit_spark_job >> add_bom >> monitoring_trusted >> csv_to_parquet >> monitoring_refined >> load_to_bq

# Instanciando a DAG
dag_instance = dag_etl_dados_publicos()
