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
PROJECT_ID = gcp_config['gcp_project_id']
REGION = gcp_config['gcp_region']
BUCKET_NAME = gcp_config['gcp_bucket_name']
CLUSTER_NAME = gcp_config['gcp_cluster_dataproc_name']
PYTHON_FILE = gcp_config['gcp_python_file_dataproc']

# Job do Spark (pode ser Python ou JAR)
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": {PYTHON_FILE}
        # "args": ["arg1", "arg2"],  # se não tiver argumentos, remova
    },
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

    scraping = PythonOperator(
        task_id="download_file",
        python_callable=download_file,
        op_kwargs={
            "project_id": PROJECT_ID,
            "bucket_name": BUCKET_NAME
        }
    )

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    add_bom = PythonOperator(
        task_id="add_bom",
        python_callable=add_bom_csv,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "project_id": PROJECT_ID,
            "folder_path": "trusted",
            
        }
    )

    scraping >> submit_spark_job >> add_bom

# Instanciando a DAG
dag_instance = dag_etl_dados_publicos()
