"""
dags/dag_credit_score_pipeline.py

DAG principal do pipeline Data Girls Finance: orquestra a extração,
transformação e carga do dataset de Credit Score Classification.

Esta DAG contém APENAS orquestração (ordem, dependências, retries).
A lógica de negócio vive nos módulos testáveis em /scripts.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Fallback para versões do Airflow onde o provider "standard" não existe
    from airflow.operators.python import PythonOperator

from scripts.extract.kaggle_extractor import extrair_dataset_kaggle
from scripts.transform.spark_cleaning import transformar_dados_credit_score
from scripts.load.s3_uploader import enviar_diretorio_para_s3

logger = logging.getLogger(__name__)

RAW_DIR = "/opt/airflow/data/raw"
TRUSTED_DIR = "/opt/airflow/data/processed/credit_score_clean"

default_args = {
    "owner": "data_girls_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def task_extrair(**kwargs):
    extrair_dataset_kaggle(diretorio_destino=RAW_DIR)


def task_transformar(**kwargs):
    transformar_dados_credit_score(
        caminho_raw=RAW_DIR,
        caminho_trusted=TRUSTED_DIR,
    )


def task_carregar(**kwargs):
    import os
    enviar_diretorio_para_s3(
        caminho_local=TRUSTED_DIR,
        nome_bucket=os.getenv("S3_BUCKET_NAME"),
    )


with DAG(
    dag_id="dag_credit_score_pipeline",
    default_args=default_args,
    description="Pipeline ETL: Kaggle -> PySpark -> S3 (Data Girls Finance)",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["bootcamp", "credit-score", "data-girls-finance"],
) as dag:

    extrair = PythonOperator(
        task_id="extrair_dados_kaggle",
        python_callable=task_extrair,
    )

    transformar = PythonOperator(
        task_id="transformar_dados_pyspark",
        python_callable=task_transformar,
    )

    carregar = PythonOperator(
        task_id="carregar_dados_s3",
        python_callable=task_carregar,
    )

    extrair >> transformar >> carregar