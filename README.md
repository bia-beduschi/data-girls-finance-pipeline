# 💰 Data Girls Finance — Pipeline ETL de Credit Score

Pipeline de dados end-to-end que extrai, transforma e carrega o dataset
[Credit Score Classification](https://www.kaggle.com/datasets/parisrohan/credit-score-classification)
do Kaggle, orquestrado por Apache Airflow, com processamento distribuído em
PySpark e armazenamento final em um Data Lake no Amazon S3.

Projeto final do Bootcamp [RE]Start — Trilha de Engenharia de Dados, para a
fintech fictícia **Data Girls Finance**.

![CI](https://github.com/bia-beduschi/data-girls-finance-pipeline/actions/workflows/ci.yml/badge.svg)

---

## 🏗️ Arquitetura

\`\`\`mermaid
flowchart LR
    A[Kaggle API] -->|extração| B[Raw Layer<br/>CSV local]
    B -->|PySpark: limpeza<br/>+ Data Quality| C[Trusted Layer<br/>Parquet particionado<br/>por Credit_Score]
    C -->|boto3 upload| D[(AWS S3<br/>Data Lake)]

    subgraph Airflow[Apache Airflow — orquestração diária]
        direction LR
        A
        B
        C
    end
\`\`\`

Todo o fluxo é orquestrado por uma **DAG do Apache Airflow** rodando em
containers Docker, com três tasks sequenciais:

| Task | Responsabilidade | Módulo |
|---|---|---|
| `extrair_dados_kaggle` | Autentica e baixa o dataset via API do Kaggle | `scripts/extract/kaggle_extractor.py` |
| `transformar_dados_pyspark` | Limpeza, padronização e validação de qualidade (Fail-Fast) | `scripts/transform/spark_cleaning.py` |
| `carregar_dados_s3` | Upload do Parquet particionado para o Data Lake (S3) | `scripts/load/s3_uploader.py` |

---

## 🛠️ Stack Técnica

- **Extração**: Python + API oficial do Kaggle
- **Transformação**: PySpark 3.5 (processamento distribuído)
- **Orquestração**: Apache Airflow 2.9 (LocalExecutor)
- **Armazenamento**: Amazon S3 (formato Parquet, particionado)
- **Containerização**: Docker + Docker Compose
- **Testes**: pytest, pytest-mock, moto (mock de AWS)
- **CI/CD**: GitHub Actions

---

## 📁 Estrutura do Repositório

\`\`\`
data-girls-finance-pipeline/
├── .github/workflows/ci.yml       # Pipeline de CI (roda os testes a cada push)
├── dags/
│   └── dag_credit_score_pipeline.py   # Orquestração das 3 tasks (Airflow)
├── scripts/
│   ├── extract/kaggle_extractor.py    # Task 1
│   ├── transform/spark_cleaning.py    # Task 2
│   └── load/s3_uploader.py            # Task 3
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── docs/
│   └── business_questions.md
├── Dockerfile                       # Imagem customizada do Airflow (+ Java p/ PySpark)
├── docker-compose.yaml              # Orquestra Postgres + Airflow (webserver/scheduler)
├── requirements.txt
├── pytest.ini
├── .env.example                     # Template de variáveis de ambiente
├── LICENSE
└── README.md
\`\`\`

---

## 🚀 Como Rodar Localmente

### Pré-requisitos
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Conta no [Kaggle](https://www.kaggle.com/) com API Token gerado
- Conta AWS com um bucket S3 e credenciais IAM com permissão de leitura/escrita no bucket

### 1. Clone o repositório
\`\`\`bash
git clone https://github.com/bia-beduschi/data-girls-finance-pipeline.git
cd data-girls-finance-pipeline
\`\`\`

### 2. Configure as variáveis de ambiente
Copie o template e preencha com suas credenciais reais:
\`\`\`bash
cp .env.example .env
\`\`\`
Edite o `.env` com:
\`\`\`
KAGGLE_USERNAME=seu_usuario_kaggle
KAGGLE_KEY=sua_chave_kaggle
AWS_ACCESS_KEY_ID=sua_chave_aws
AWS_SECRET_ACCESS_KEY=sua_secret_aws
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=seu_bucket_s3
AIRFLOW_UID=50000
\`\`\`

> ⚠️ O `.env` nunca deve ser commitado — já está protegido pelo `.gitignore`.

### 3. Suba o ambiente Airflow
\`\`\`bash
docker compose up airflow-init   # inicializa o banco de metadados e cria o usuário admin
docker compose up -d             # sobe webserver + scheduler
\`\`\`

### 4. Acesse a interface do Airflow
http://localhost:8080

Login: `admin` / `admin`

Ative a DAG `dag_credit_score_pipeline` e dispare manualmente (▶️) ou aguarde
o agendamento diário automático.

---

## 🧪 Rodando os Testes e os Scripts Localmente (fora do Docker)

Para testar os scripts individualmente sem subir o Airflow, crie um
ambiente virtual e instale exatamente as versões travadas no
`requirements.txt` — isso evita divergências de versão (por exemplo, do
PySpark) que podem gerar comportamentos inesperados fora do ambiente
validado:

\`\`\`bash
python -m venv .venv
.venv\\Scripts\\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -r requirements.txt
pytest -v
\`\`\`

A suíte cobre as 3 tasks com **19 testes automatizados**, usando mocks para
a API do Kaggle e a biblioteca `moto` para simular o S3 — nenhum teste
depende de credenciais reais ou acesso à internet.

O CI (GitHub Actions) roda essa mesma suíte automaticamente a cada `push`
ou `pull request` na branch `main`.

> **Nota sobre execução local no Windows fora do Docker**: rodar
> `scripts/transform/spark_cleaning.py` diretamente no Windows (fora do
> container) pode exigir configuração adicional do Hadoop (`winutils.exe`),
> uma limitação conhecida do PySpark nesse sistema operacional. Dentro do
> ambiente Docker (Linux), usado pela DAG em produção, essa limitação não
> existe.

---

## 📊 Sobre o Dataset

[Credit Score Classification](https://www.kaggle.com/datasets/parisrohan/credit-score-classification)
— dataset sintético com informações demográficas, financeiras e de
comportamento de pagamento de clientes, usado para prever a classe de
score de crédito (`Good`, `Standard`, `Poor`).

O dataset bruto contém diversas inconsistências propositais (idades
negativas ou absurdas, valores numéricos com underscores, placeholders
textuais como `"_______"`), tratadas explicitamente na Task 2.

---

## 📜 Respostas às Perguntas Norteadoras de Negócio

As respostas completas e fundamentadas estão em [`docs/business_questions.md`](docs/business_questions.md).

---

## 👩‍💻 Autora

Beatriz Beduschi — Projeto final da Trilha de Engenharia de Dados, Bootcamp [RE]Start.