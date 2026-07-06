"""
tests/test_transform.py

Testes unitários da Task 2 (limpeza e validação de qualidade com PySpark).

Estratégia: em vez de ler o CSV real de 100 mil linhas, criamos DataFrames
pequenos (2 a 5 linhas) diretamente em memória, com exatamente os cenários
de "sujeira" que sabemos existir no dataset (idade negativa, underscore em
números, placeholders como '_______'). Isso torna os testes rápidos
(rodam em segundos) e focados em uma regra de negócio por vez.
"""

import pytest
from pyspark.sql import SparkSession
import sys

from scripts.transform.spark_cleaning import (
    DataQualityError,
    limpar_idade,
    limpar_num_of_loan,
    limpar_outstanding_debt,
    limpar_renda_anual,
    padronizar_categoricas_com_placeholder,
    validar_qualidade_dados,
)


@pytest.fixture(scope="session")
def spark():
    """
    Cria UMA ÚNICA SparkSession compartilhada por todos os testes deste
    arquivo, evitando o custo de inicializar o Spark repetidamente.
    """
    sessao = (
        SparkSession.builder.appName("testes_credit_score_cleaning")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


class TestLimparIdade:
    def test_remove_underscore_e_converte_para_numero(self, spark):
        df = spark.createDataFrame([("25_",), ("40",)], ["Age"])
        resultado = limpar_idade(df).collect()
        assert resultado[0]["Age"] == 25.0
        assert resultado[1]["Age"] == 40.0

    def test_idade_fora_do_intervalo_valido_vira_nulo(self, spark):
        df = spark.createDataFrame([("150",), ("-5",), ("0",), ("100",)], ["Age"])
        resultado = limpar_idade(df).collect()
        assert resultado[0]["Age"] is None
        assert resultado[1]["Age"] is None
        assert resultado[2]["Age"] is None
        assert resultado[3]["Age"] == 100.0


class TestLimparRendaAnual:
    def test_renda_negativa_vira_nula(self, spark):
        df = spark.createDataFrame([("-1000",), ("5000",)], ["Annual_Income"])
        resultado = limpar_renda_anual(df).collect()
        assert resultado[0]["Annual_Income"] is None
        assert resultado[1]["Annual_Income"] == 5000.0


class TestLimparNumOfLoan:
    def test_valores_fora_do_intervalo_zero_a_dez_viram_nulos(self, spark):
        df = spark.createDataFrame([("15",), ("5",), ("-2",)], ["Num_of_Loan"])
        resultado = limpar_num_of_loan(df).collect()
        assert resultado[0]["Num_of_Loan"] is None
        assert resultado[1]["Num_of_Loan"] == 5.0
        assert resultado[2]["Num_of_Loan"] is None


class TestLimparOutstandingDebt:
    def test_divida_negativa_vira_nula(self, spark):
        df = spark.createDataFrame([("-500.0",), ("1200.5",)], ["Outstanding_Debt"])
        resultado = limpar_outstanding_debt(df).collect()
        assert resultado[0]["Outstanding_Debt"] is None
        assert resultado[1]["Outstanding_Debt"] == 1200.5


class TestPadronizarCategoricas:
    def test_placeholder_vira_unknown(self, spark):
        df = spark.createDataFrame(
            [("_______", "_", "!@9#%8"), ("Engineer", "Good", "High_spent_Small_value")],
            ["Occupation", "Credit_Mix", "Payment_Behaviour"],
        )
        resultado = padronizar_categoricas_com_placeholder(df).collect()
        assert resultado[0]["Occupation"] == "Unknown"
        assert resultado[0]["Credit_Mix"] == "Unknown"
        assert resultado[0]["Payment_Behaviour"] == "Unknown"
        assert resultado[1]["Occupation"] == "Engineer"


class TestValidarQualidadeDados:
    def test_levanta_erro_quando_customer_id_e_nulo(self, spark):
        df = spark.createDataFrame(
            [(None, 50000.0, "STANDARD"), ("CUS001", 60000.0, "GOOD")],
            ["Customer_ID", "Annual_Income", "Credit_Score"],
        )
        with pytest.raises(DataQualityError, match="Customer_ID nulo"):
            validar_qualidade_dados(df)

    def test_levanta_erro_quando_renda_negativa_residual(self, spark):
        df = spark.createDataFrame(
            [("CUS001", -100.0, "STANDARD")],
            ["Customer_ID", "Annual_Income", "Credit_Score"],
        )
        with pytest.raises(DataQualityError, match="renda negativa"):
            validar_qualidade_dados(df)

    def test_nao_levanta_erro_com_dados_validos(self, spark):
        df = spark.createDataFrame(
            [("CUS001", 50000.0, "STANDARD"), ("CUS002", 60000.0, "GOOD")],
            ["Customer_ID", "Annual_Income", "Credit_Score"],
        )
        validar_qualidade_dados(df)

    def test_levanta_erro_com_dataframe_vazio(self, spark):
        df = spark.createDataFrame([], "Customer_ID STRING, Annual_Income DOUBLE, Credit_Score STRING")
        with pytest.raises(DataQualityError, match="vazio"):
            validar_qualidade_dados(df)