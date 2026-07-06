"""
scripts/transform/spark_cleaning.py

Task 2 do pipeline Data Girls Finance: responsável por ler os dados brutos
(camada Raw), aplicar limpeza, validações de qualidade (Fail-Fast) e
persistir o resultado em Parquet particionado (camada Trusted).
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Erro customizado levantado quando uma regra crítica de qualidade falha."""


# ------------------------------------------------------------------
# SCHEMA EXPLÍCITO
# ------------------------------------------------------------------
# Definir o schema manualmente evita que o Spark infira tipos incorretamente
# (ex: uma coluna numérica com "_" no meio vira string, e é isso que
# queremos aqui — controlamos a conversão de tipos nós mesmos, de forma
# explícita, na etapa de limpeza).
def construir_schema_credit_score() -> StructType:
    """Define o schema de leitura do CSV bruto do Kaggle."""
    colunas = [
        "ID", "Customer_ID", "Month", "Name", "Age", "SSN", "Occupation",
        "Annual_Income", "Monthly_Inhand_Salary", "Num_Bank_Accounts",
        "Num_Credit_Card", "Interest_Rate", "Num_of_Loan", "Type_of_Loan",
        "Delay_from_due_date", "Num_of_Delayed_Payment", "Changed_Credit_Limit",
        "Num_Credit_Inquiries", "Credit_Mix", "Outstanding_Debt",
        "Credit_Utilization_Ratio", "Credit_History_Age",
        "Payment_of_Min_Amount", "Total_EMI_per_month",
        "Amount_invested_monthly", "Payment_Behaviour", "Monthly_Balance",
        "Credit_Score",
    ]
    # Lemos tudo como String de propósito: os dados vêm sujos (ex: "25_",
    # "_______"), então tipar como IntegerType/DoubleType direto na leitura
    # faria o Spark descartar valores como NULL silenciosamente.
    return StructType([StructField(c, StringType(), True) for c in colunas])


def criar_spark_session(nome_app: str = "data_girls_finance_cleaning") -> SparkSession:
    """
    Cria (ou recupera) a SparkSession configurada para execução local.

    Em produção (EMR, Databricks, etc.) o master seria substituído pelo
    endereço do cluster, mas para o escopo deste bootcamp usamos local[*],
    que utiliza todos os núcleos disponíveis da máquina.
    """
    try:
        spark = (
            SparkSession.builder.appName(nome_app)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")  # reduz overhead em dataset pequeno
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")  # evita poluir o log com INFO do Spark
        return spark
    except Exception as erro:
        raise RuntimeError(
            f"Falha ao inicializar a SparkSession. Verifique se o Java (JDK) "
            f"está instalado e JAVA_HOME configurado. Detalhe: {erro}"
        ) from erro


# ------------------------------------------------------------------
# FUNÇÕES DE LIMPEZA (uma responsabilidade cada, testáveis isoladamente)
# ------------------------------------------------------------------
def limpar_coluna_numerica_suja(df: DataFrame, coluna: str) -> DataFrame:
    """
    Remove underscores e espaços de uma coluna string e converte para double.
    Valores não conversíveis viram NULL (equivalente ao errors='coerce' do pandas).
    """
    return df.withColumn(
        coluna,
        F.regexp_replace(F.col(coluna), "_", "").cast("double"),
    )


def limpar_idade(df: DataFrame) -> DataFrame:
    """
    Regra de negócio: idade válida está entre 1 e 100 anos.
    Fora desse intervalo, tratamos como dado corrompido (NULL) para
    posterior imputação, em vez de manter um valor absurdo no dataset.
    """
    df = limpar_coluna_numerica_suja(df, "Age")
    return df.withColumn(
        "Age",
        F.when((F.col("Age") >= 1) & (F.col("Age") <= 100), F.col("Age")).otherwise(None),
    )


def limpar_renda_anual(df: DataFrame) -> DataFrame:
    """Converte Annual_Income para double e remove valores negativos."""
    df = limpar_coluna_numerica_suja(df, "Annual_Income")
    return df.withColumn(
        "Annual_Income",
        F.when(F.col("Annual_Income") >= 0, F.col("Annual_Income")).otherwise(None),
    )


def limpar_num_of_loan(df: DataFrame) -> DataFrame:
    """Número de empréstimos válido: entre 0 e 10 (fora disso é outlier/corrompido)."""
    df = limpar_coluna_numerica_suja(df, "Num_of_Loan")
    return df.withColumn(
        "Num_of_Loan",
        F.when((F.col("Num_of_Loan") >= 0) & (F.col("Num_of_Loan") <= 10), F.col("Num_of_Loan")).otherwise(None),
    )


def limpar_outstanding_debt(df: DataFrame) -> DataFrame:
    """Converte Outstanding_Debt para double; dívida negativa não faz sentido de negócio."""
    df = limpar_coluna_numerica_suja(df, "Outstanding_Debt")
    return df.withColumn(
        "Outstanding_Debt",
        F.when(F.col("Outstanding_Debt") >= 0, F.col("Outstanding_Debt")).otherwise(None),
    )


def padronizar_categoricas_com_placeholder(df: DataFrame) -> DataFrame:
    """
    Substitui placeholders de dado ausente conhecidos no dataset original
    (identificados na fase de EDA) por um rótulo explícito 'Unknown', em vez
    de deixá-los como lixo textual que confundiria encoders categóricos.
    """
    mapeamentos = {
        "Occupation": "_______",
        "Credit_Mix": "_",
        "Payment_Behaviour": "!@9#%8",
    }
    for coluna, placeholder in mapeamentos.items():
        df = df.withColumn(
            coluna,
            F.when(F.col(coluna) == placeholder, "Unknown").otherwise(F.col(coluna)),
        )
    return df


def padronizar_credit_score(df: DataFrame) -> DataFrame:
    """Normaliza a coluna target: remove espaços e uniformiza caixa alta."""
    return df.withColumn(
        "Credit_Score",
        F.upper(F.trim(F.col("Credit_Score"))),
    )


# ------------------------------------------------------------------
# CAMADA DE VALIDAÇÃO DE QUALIDADE (FAIL-FAST)
# ------------------------------------------------------------------
def validar_qualidade_dados(df: DataFrame) -> None:
    """
    Executa checagens críticas de qualidade. Se qualquer uma falhar,
    o pipeline é interrompido ANTES de gravar dados no Data Lake —
    evitando que informação corrompida chegue às equipes de negócio.

    Raises:
        DataQualityError: se alguma regra crítica for violada.
    """
    total_linhas = df.count()
    if total_linhas == 0:
        raise DataQualityError("Dataset resultante está vazio. Abortando pipeline.")

    ids_nulos = df.filter(F.col("Customer_ID").isNull()).count()
    logger.info("Customer_ID nulos encontrados: %d", ids_nulos)
    if ids_nulos > 0:
        raise DataQualityError(
            f"Falha crítica de qualidade: {ids_nulos} registros com Customer_ID nulo."
        )

    rendas_negativas = df.filter(F.col("Annual_Income") < 0).count()
    if rendas_negativas > 0:
        raise DataQualityError(
            f"Falha crítica de qualidade: {rendas_negativas} registros com renda negativa residual."
        )

    scores_invalidos = df.filter(
        ~F.col("Credit_Score").isin(["GOOD", "STANDARD", "POOR"])
    ).count()
    logger.info("Registros com Credit_Score fora do domínio esperado: %d", scores_invalidos)

    logger.info("Validação de qualidade concluída com sucesso. Total de linhas: %d", total_linhas)


# ------------------------------------------------------------------
# ORQUESTRAÇÃO DA TASK (função de entrada, chamada pela DAG)
# ------------------------------------------------------------------
def transformar_dados_credit_score(caminho_raw: str, caminho_trusted: str) -> str:
    """
    Função principal da Task 2: lê o CSV bruto, aplica todas as regras de
    limpeza, valida qualidade e persiste em Parquet particionado por
    Credit_Score.

    Args:
        caminho_raw: diretório contendo o train.csv bruto.
        caminho_trusted: diretório de destino para o Parquet limpo.

    Returns:
        O caminho onde os dados trusted foram salvos.
    """
    logger.info("Iniciando Task 2 - Transformação e Validação com PySpark")

    spark = criar_spark_session()

    try:
        caminho_arquivo = os.path.join(caminho_raw, "train.csv")
        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")

        df = spark.read.csv(
            caminho_arquivo,
            header=True,
            schema=construir_schema_credit_score(),
        )

        logger.info("Aplicando regras de limpeza...")
        df = limpar_idade(df)
        df = limpar_renda_anual(df)
        df = limpar_num_of_loan(df)
        df = limpar_outstanding_debt(df)
        df = padronizar_categoricas_com_placeholder(df)
        df = padronizar_credit_score(df)

        logger.info("Executando validação de qualidade (Fail-Fast)...")
        validar_qualidade_dados(df)

        logger.info("Gravando dados trusted em Parquet particionado em: %s", caminho_trusted)
        (
            df.write.mode("overwrite")
            .partitionBy("Credit_Score")
            .parquet(caminho_trusted)
        )

        logger.info("Task 2 concluída com sucesso.")
        return caminho_trusted

    finally:
        # Encerrar a sessão sempre, mesmo se algo falhar, para liberar
        # recursos (memória/threads) do driver Spark.
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    transformar_dados_credit_score(
        caminho_raw="./data/raw",
        caminho_trusted="./data/processed/credit_score_clean",
    )