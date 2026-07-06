"""
scripts/extract/kaggle_extractor.py

Task 1 do pipeline Data Girls Finance: responsável por extrair o dataset
'Credit Score Classification' diretamente da API do Kaggle e armazená-lo
na camada Raw local, sem qualquer transformação.
"""

import logging
import os
import zipfile

logger = logging.getLogger(__name__)

DATASET_SLUG = "parisrohan/credit-score-classification"


class KaggleExtractionError(Exception):
    """Erro customizado para falhas na etapa de extração via Kaggle API."""


def _validar_credenciais_kaggle() -> None:
    """
    Garante que as credenciais necessárias existem no ambiente antes de
    tentar autenticar. Falhar aqui é mais barato e mais claro do que deixar
    a lib do Kaggle estourar um erro genérico lá na frente.
    """
    variaveis_obrigatorias = ["KAGGLE_USERNAME", "KAGGLE_KEY"]
    faltantes = [v for v in variaveis_obrigatorias if not os.getenv(v)]

    if faltantes:
        raise KaggleExtractionError(
            f"Credenciais do Kaggle ausentes no ambiente: {faltantes}. "
            "Configure-as como Airflow Variables/Connections em produção "
            "ou no .env em ambiente local."
        )


def extrair_dataset_kaggle(diretorio_destino: str) -> str:
    """
    Autentica na API do Kaggle, baixa o dataset de classificação de score
    de crédito e o descompacta no diretório de destino (camada Raw).

    Args:
        diretorio_destino: caminho local onde os arquivos brutos serão salvos.

    Returns:
        O caminho absoluto do diretório onde os dados foram extraídos.

    Raises:
        KaggleExtractionError: se credenciais estiverem ausentes, o download
            falhar, ou o arquivo zip estiver corrompido/ausente.
    """
    logger.info("Iniciando Task 1 - Extração via Kaggle API")

    _validar_credenciais_kaggle()

    # Import tardio: evita custo de inicialização da lib quando o módulo
    # é apenas importado (ex: durante testes que fazem mock desta função)
    from kaggle.api.kaggle_api_extended import KaggleApi

    try:
        api = KaggleApi()
        api.authenticate()
    except Exception as erro:
        raise KaggleExtractionError(
            f"Falha na autenticação com a API do Kaggle: {erro}"
        ) from erro

    os.makedirs(diretorio_destino, exist_ok=True)

    try:
        logger.info("Baixando dataset '%s'...", DATASET_SLUG)
        api.dataset_download_files(
            DATASET_SLUG, path=diretorio_destino, unzip=False
        )
    except Exception as erro:
        raise KaggleExtractionError(
            f"Falha ao baixar o dataset '{DATASET_SLUG}': {erro}"
        ) from erro

    zip_path = os.path.join(diretorio_destino, "credit-score-classification.zip")

    if not os.path.exists(zip_path):
        raise KaggleExtractionError(
            f"Arquivo zip esperado não foi encontrado em: {zip_path}"
        )

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(diretorio_destino)
    except zipfile.BadZipFile as erro:
        raise KaggleExtractionError(
            f"Arquivo zip corrompido, extração abortada: {erro}"
        ) from erro
    finally:
        # Remove o zip mesmo se a extração falhar parcialmente,
        # para não deixar lixo de execuções anteriores.
        if os.path.exists(zip_path):
            os.remove(zip_path)

    logger.info(
        "Task 1 concluída com sucesso. Dados brutos disponíveis em: %s",
        diretorio_destino,
    )
    return diretorio_destino


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    extrair_dataset_kaggle(diretorio_destino="./data/raw")