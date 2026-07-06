"""
scripts/load/s3_uploader.py

Task 3 do pipeline Data Girls Finance: responsável por sincronizar os
arquivos Parquet da camada Trusted local com o bucket S3 do Data Lake.
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError

logger = logging.getLogger(__name__)


class S3UploadError(Exception):
    """Erro customizado para falhas na etapa de carga para o S3."""


def _criar_cliente_s3():
    """
    Cria o cliente boto3 do S3. As credenciais NÃO são passadas explicitamente
    aqui — o boto3 resolve automaticamente via variáveis de ambiente
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) ou, em produção, via IAM Role
    anexada à instância/task do Airflow (a forma mais segura, sem chave
    nenhuma em lugar nenhum).
    """
    try:
        return boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
    except Exception as erro:
        raise S3UploadError(f"Falha ao inicializar cliente S3: {erro}") from erro


def _bucket_existe(cliente_s3, nome_bucket: str) -> bool:
    """Verifica se o bucket já existe e está acessível com as credenciais atuais."""
    try:
        cliente_s3.head_bucket(Bucket=nome_bucket)
        return True
    except ClientError as erro:
        codigo_erro = erro.response.get("Error", {}).get("Code", "")
        if codigo_erro in ("404", "NoSuchBucket"):
            return False
        # Qualquer outro código (403 Forbidden, por exemplo) indica um
        # problema de permissão que precisa ser tratado explicitamente,
        # não silenciosamente assumido como "bucket não existe".
        raise S3UploadError(
            f"Erro ao verificar o bucket '{nome_bucket}': {erro}"
        ) from erro


def _garantir_bucket(cliente_s3, nome_bucket: str, regiao: str) -> None:
    """
    Garante que o bucket existe, criando-o caso necessário.

    Nota de arquitetura: em ambientes corporativos maduros, a criação de
    infraestrutura (buckets, políticas de IAM) normalmente é gerida via
    Infraestrutura como Código (Terraform/CloudFormation), não pelo próprio
    pipeline de dados. Mantemos a criação aqui para fins didáticos do
    bootcamp, mas vale registrar essa ressalva na documentação do projeto.
    """
    if _bucket_existe(cliente_s3, nome_bucket):
        logger.info("Bucket '%s' já existe. Prosseguindo com upload.", nome_bucket)
        return

    logger.info("Bucket '%s' não encontrado. Criando...", nome_bucket)
    try:
        if regiao == "us-east-1":
            # A API da AWS trata us-east-1 como caso especial: não aceita
            # o parâmetro LocationConstraint para essa região específica.
            cliente_s3.create_bucket(Bucket=nome_bucket)
        else:
            cliente_s3.create_bucket(
                Bucket=nome_bucket,
                CreateBucketConfiguration={"LocationConstraint": regiao},
            )
        logger.info("Bucket '%s' criado com sucesso.", nome_bucket)
    except ClientError as erro:
        raise S3UploadError(
            f"Falha ao criar o bucket '{nome_bucket}': {erro}"
        ) from erro


def enviar_diretorio_para_s3(
    caminho_local: str,
    nome_bucket: str,
    prefixo_s3: str = "credit_score_clean",
) -> int:
    """
    Percorre recursivamente o diretório local (Parquet particionado) e
    replica a mesma estrutura de pastas como chaves no S3.

    Args:
        caminho_local: diretório local contendo os arquivos .parquet.
        nome_bucket: nome do bucket de destino no S3.
        prefixo_s3: prefixo (pasta lógica) dentro do bucket.

    Returns:
        Número de arquivos efetivamente enviados.

    Raises:
        S3UploadError: se credenciais estiverem ausentes, o bucket for
            inacessível, ou nenhum arquivo .parquet for encontrado.
    """
    logger.info("Iniciando Task 3 - Upload para o S3")

    if not os.path.exists(caminho_local):
        raise S3UploadError(f"Diretório local não encontrado: {caminho_local}")

    cliente_s3 = _criar_cliente_s3()
    regiao = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    try:
        _garantir_bucket(cliente_s3, nome_bucket, regiao)
    except NoCredentialsError as erro:
        raise S3UploadError(
            "Credenciais da AWS não encontradas. Configure "
            "AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY como variáveis de "
            "ambiente (ou Connections do Airflow em produção)."
        ) from erro

    arquivos_enviados = 0
    arquivos_com_falha = []

    for raiz, _, arquivos in os.walk(caminho_local):
        for nome_arquivo in arquivos:
            if not nome_arquivo.endswith(".parquet"):
                continue

            caminho_completo = os.path.join(raiz, nome_arquivo)
            caminho_relativo = os.path.relpath(caminho_completo, caminho_local)
            chave_s3 = f"{prefixo_s3}/{caminho_relativo.replace(os.sep, '/')}"

            try:
                cliente_s3.upload_file(caminho_completo, nome_bucket, chave_s3)
                logger.info("Arquivo enviado: s3://%s/%s", nome_bucket, chave_s3)
                arquivos_enviados += 1
            except (ClientError, ParamValidationError) as erro:
                logger.error("Falha ao enviar '%s': %s", chave_s3, erro)
                arquivos_com_falha.append(chave_s3)

    if arquivos_enviados == 0:
        raise S3UploadError(
            f"Nenhum arquivo .parquet encontrado em '{caminho_local}' para envio."
        )

    if arquivos_com_falha:
        raise S3UploadError(
            f"Task 3 finalizada com falhas parciais. "
            f"{len(arquivos_com_falha)} arquivo(s) não enviados: {arquivos_com_falha}"
        )

    logger.info(
        "Task 3 concluída com sucesso. %d arquivo(s) enviados para s3://%s/%s",
        arquivos_enviados, nome_bucket, prefixo_s3,
    )
    return arquivos_enviados


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    enviar_diretorio_para_s3(
        caminho_local="./data/processed/credit_score_clean",
        nome_bucket=os.getenv("S3_BUCKET_NAME", "data-girls-finance-trusted-bucket-davis"),
    )