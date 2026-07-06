"""
tests/test_load.py

Testes unitários da Task 3 (upload para o S3).

Estratégia: usamos a biblioteca `moto`, que simula a API da AWS inteira em
memória, sem fazer nenhuma chamada de rede real e sem precisar de
credenciais verdadeiras. Isso permite testar o comportamento do nosso
código (criação de bucket, upload, tratamento de erros) com total
segurança e velocidade.
"""

import os

import boto3
import pytest
from moto import mock_aws

from scripts.load.s3_uploader import S3UploadError, enviar_diretorio_para_s3


@pytest.fixture
def bucket_de_teste():
    """
    Fixture que fornece um ambiente AWS "de mentira" (moto), com um bucket
    já criado, pronto para receber uploads durante o teste.
    """
    with mock_aws():
        os.environ["AWS_ACCESS_KEY_ID"] = "chave_fake_para_teste"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "segredo_fake_para_teste"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        cliente = boto3.client("s3", region_name="us-east-1")
        nome_bucket = "bucket-teste-data-girls-finance"
        cliente.create_bucket(Bucket=nome_bucket)

        yield nome_bucket


class TestEnviarDiretorioParaS3:
    def test_envia_arquivos_parquet_com_sucesso(self, bucket_de_teste, tmp_path):
        # Arrange: cria uma estrutura de pastas/arquivos falsa, simulando
        # o Parquet particionado que o Spark gera de verdade.
        pasta_particao = tmp_path / "Credit_Score=GOOD"
        pasta_particao.mkdir()
        arquivo_parquet = pasta_particao / "part-00000.snappy.parquet"
        arquivo_parquet.write_bytes(b"conteudo fake de parquet")

        # Act
        total_enviados = enviar_diretorio_para_s3(
            caminho_local=str(tmp_path),
            nome_bucket=bucket_de_teste,
            prefixo_s3="credit_score_clean",
        )

        # Assert
        assert total_enviados == 1

        cliente = boto3.client("s3", region_name="us-east-1")
        objetos = cliente.list_objects_v2(Bucket=bucket_de_teste)
        chaves = [obj["Key"] for obj in objetos.get("Contents", [])]
        assert "credit_score_clean/Credit_Score=GOOD/part-00000.snappy.parquet" in chaves

    def test_falha_quando_diretorio_local_nao_existe(self, bucket_de_teste):
        with pytest.raises(S3UploadError, match="Diretório local não encontrado"):
            enviar_diretorio_para_s3(
                caminho_local="/caminho/que/nao/existe",
                nome_bucket=bucket_de_teste,
            )

    def test_falha_quando_nenhum_arquivo_parquet_encontrado(self, bucket_de_teste, tmp_path):
        # Cria um arquivo que NÃO é .parquet, para confirmar que é ignorado
        (tmp_path / "arquivo_irrelevante.txt").write_text("nao deveria ser enviado")

        with pytest.raises(S3UploadError, match="Nenhum arquivo .parquet encontrado"):
            enviar_diretorio_para_s3(
                caminho_local=str(tmp_path),
                nome_bucket=bucket_de_teste,
            )

    def test_cria_bucket_automaticamente_quando_nao_existe(self, tmp_path):
        with mock_aws():
            os.environ["AWS_ACCESS_KEY_ID"] = "chave_fake_para_teste"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "segredo_fake_para_teste"
            os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

            arquivo_parquet = tmp_path / "part-00000.snappy.parquet"
            arquivo_parquet.write_bytes(b"conteudo fake")

            nome_bucket_novo = "bucket-que-ainda-nao-existe"
            total_enviados = enviar_diretorio_para_s3(
                caminho_local=str(tmp_path),
                nome_bucket=nome_bucket_novo,
            )

            assert total_enviados == 1
            cliente = boto3.client("s3", region_name="us-east-1")
            # Confirma que o bucket foi criado automaticamente pelo nosso código
            buckets = [b["Name"] for b in cliente.list_buckets()["Buckets"]]
            assert nome_bucket_novo in buckets