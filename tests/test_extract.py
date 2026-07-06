"""
tests/test_extract.py

Testes unitários da Task 1 (extração Kaggle).

Estratégia: NUNCA chamamos a API real do Kaggle nos testes — usamos mocks
para simular tanto o cenário de sucesso quanto os cenários de falha
(credenciais ausentes, falha de download, zip corrompido). Isso torna os
testes rápidos, determinísticos e seguros para rodar em qualquer ambiente
(inclusive no CI, onde não há credenciais reais).
"""

from unittest.mock import MagicMock, patch

import pytest

from scripts.extract.kaggle_extractor import (
    KaggleExtractionError,
    _validar_credenciais_kaggle,
    extrair_dataset_kaggle,
)


class TestValidarCredenciaisKaggle:
    """Testes da função de validação de credenciais (isolada, sem I/O)."""

    def test_levanta_erro_quando_credenciais_ausentes(self, monkeypatch):
        # Arrange: garante que as variáveis não existem no ambiente do teste
        monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
        monkeypatch.delenv("KAGGLE_KEY", raising=False)

        # Act & Assert
        with pytest.raises(KaggleExtractionError, match="Credenciais do Kaggle ausentes"):
            _validar_credenciais_kaggle()

    def test_nao_levanta_erro_quando_credenciais_presentes(self, monkeypatch):
        monkeypatch.setenv("KAGGLE_USERNAME", "usuario_teste")
        monkeypatch.setenv("KAGGLE_KEY", "chave_teste")

        # Não deve lançar nenhuma exceção
        _validar_credenciais_kaggle()


class TestExtrairDatasetKaggle:
    """Testes da função principal de extração, com a API do Kaggle mockada."""

    def test_falha_quando_credenciais_ausentes(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
        monkeypatch.delenv("KAGGLE_KEY", raising=False)

        with pytest.raises(KaggleExtractionError):
            extrair_dataset_kaggle(diretorio_destino=str(tmp_path))

    @patch("kaggle.api.kaggle_api_extended.KaggleApi")
    def test_falha_quando_autenticacao_da_api_quebra(
        self, mock_kaggle_api_classe, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("KAGGLE_USERNAME", "usuario_teste")
        monkeypatch.setenv("KAGGLE_KEY", "chave_teste")

        # Simula a API do Kaggle lançando erro na autenticação
        instancia_mock = MagicMock()
        instancia_mock.authenticate.side_effect = Exception("Token inválido")
        mock_kaggle_api_classe.return_value = instancia_mock

        with pytest.raises(KaggleExtractionError, match="Falha na autenticação"):
            extrair_dataset_kaggle(diretorio_destino=str(tmp_path))

    @patch("kaggle.api.kaggle_api_extended.KaggleApi")
    def test_falha_quando_zip_nao_e_criado(
        self, mock_kaggle_api_classe, tmp_path, monkeypatch
    ):
        """
        Simula o caso em que a API 'autentica' e 'baixa' com sucesso, mas o
        arquivo zip esperado não aparece no disco (ex: dataset removido,
        nome do slug incorreto).
        """
        monkeypatch.setenv("KAGGLE_USERNAME", "usuario_teste")
        monkeypatch.setenv("KAGGLE_KEY", "chave_teste")

        instancia_mock = MagicMock()
        instancia_mock.authenticate.return_value = None
        instancia_mock.dataset_download_files.return_value = None
        mock_kaggle_api_classe.return_value = instancia_mock

        with pytest.raises(KaggleExtractionError, match="zip esperado não foi encontrado"):
            extrair_dataset_kaggle(diretorio_destino=str(tmp_path))