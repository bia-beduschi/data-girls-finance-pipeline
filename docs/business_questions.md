# 📜 Perguntas Norteadoras de Negócio

Este documento responde às 4 perguntas norteadoras do briefing do projeto,
conectando cada decisão de arquitetura tomada no pipeline a um motivo de
negócio concreto para a fintech fictícia **Data Girls Finance**.

---

## 1. Como garantir que os dados cadastrais e financeiros dos clientes estejam sempre atualizados e prontos para utilização pelas equipes de negócio?

A atualização contínua é garantida pela **orquestração via Apache Airflow**,
não por execução manual:

- A DAG `dag_credit_score_pipeline` roda com `schedule="@daily"`, disparando
  automaticamente as 3 tasks (Extração → Transformação → Carga) todos os
  dias, sem intervenção humana.
- `catchup=False` evita que o Airflow tente "recuperar" execuções passadas
  ao ativar a DAG pela primeira vez — rodamos apenas a partir do momento
  presente, evitando processamento redundante de janelas de tempo que não
  fazem sentido de negócio para esse cenário.
- `retries=2` com `retry_delay=timedelta(minutes=5)` garante resiliência a
  falhas transitórias (ex: instabilidade momentânea da API do Kaggle ou do
  S3), sem exigir que alguém do time perceba a falha e dispare manualmente
  de novo.
- **Logging estruturado** em cada task (extração, transformação, carga)
  permite que qualquer falha seja identificada rapidamente na UI do Airflow,
  reduzindo o tempo entre "algo quebrou" e "alguém sabe disso" — essencial
  para um pipeline que alimenta decisões de crédito.
- Cada execução é **idempotente** (ver pergunta 3): mesmo que a DAG rode
  mais de uma vez no mesmo dia (retry automático, disparo manual de teste),
  o resultado final é sempre consistente, sem duplicar ou corromper dados.

**Próximo passo de maturidade** (fora do escopo atual, mas documentado como
evolução natural): migrar de uma extração *full* (baixa a base inteira todo
dia) para uma extração **incremental**, trazendo apenas clientes
novos/alterados desde a última execução — reduz custo de processamento e
tempo de pipeline à medida que a base cresce.

---

## 2. Quais validações de qualidade dos dados devem ser realizadas antes que as informações sejam disponibilizadas para análises e modelos de score de crédito?

O pipeline aplica uma filosofia de **Fail-Fast**: se uma regra crítica de
qualidade falhar, a Task 2 é interrompida **antes** de qualquer dado chegar
à camada Trusted ou ao S3 — nenhuma equipe de negócio jamais consome um
dado que não passou pelo crivo de qualidade.

As validações aplicadas (função `validar_qualidade_dados`) incluem:

- **Schema explícito na leitura**: todas as colunas são lidas como `String`
  propositalmente, e convertidas para o tipo correto (`double`, `int`) de
  forma controlada na limpeza — evitando que o Spark descarte silenciosamente
  valores sujos (`"25_"`, `"_______"`) ao tentar inferir tipos sozinho.
- **Regras de domínio de negócio**, aplicadas coluna a coluna:
  - `Age`: apenas valores entre 1 e 100 são aceitos; fora disso, vira `NULL`
    para tratamento posterior (não descartamos a linha inteira, só o dado
    corrompido).
  - `Annual_Income` e `Outstanding_Debt`: valores negativos não fazem
    sentido de negócio (não existe renda ou dívida negativa) e são tratados
    como inválidos.
  - `Num_of_Loan`: fora do intervalo 0–10 é tratado como outlier/corrupção
    de digitação.
  - Placeholders textuais conhecidos (`"_______"`, `"_"`, `"!@9#%8"`) são
    padronizados para `"Unknown"`, evitando lixo textual em análises e
    encoders categóricos.
- **Checagens críticas (fail-fast)**, que interrompem o pipeline caso
  falhem:
  - Dataset resultante não pode estar vazio.
  - Nenhum registro pode ter `Customer_ID` nulo (é a chave de identificação
    do cliente — sem ela, o registro é inútil para qualquer análise
    posterior).
  - Não pode haver renda residual negativa após a limpeza (indicaria falha
    na própria lógica de tratamento).
- **Conformidade com privacidade (LGPD)**: colunas de dado pessoal
  identificável (`Name`, `SSN`) são removidas antes da persistência —
  o Data Lake não deve reter PII desnecessário para a finalidade analítica
  do pipeline.

---

## 3. Como estruturar um pipeline que permita atualizações periódicas dos dados sem duplicar registros e preservando sua consistência?

A resposta central é **idempotência de escrita**:

- A Task 2 grava o resultado com `.write.mode("overwrite")` — cada execução
  **substitui** o conteúdo da camada Trusted, em vez de anexar (`append`)
  novos dados por cima dos antigos. Rodar a DAG duas vezes no mesmo dia (por
  retry, por teste manual, por reprocessamento) produz exatamente o mesmo
  resultado final, sem duplicação.
- A Task 3 usa um **prefixo determinístico e fixo** no S3
  (`credit_score_clean/...`), nunca um caminho com timestamp aleatório —
  isso significa que o upload também sobrescreve o conteúdo anterior de
  forma previsível, em vez de acumular versões órfãs no bucket ao longo do
  tempo.
- O particionamento por `Credit_Score` (`GOOD` / `STANDARD` / `POOR`)
  organiza fisicamente os dados em subpastas dentro do mesmo prefixo — uma
  nova execução recria essas partições de forma consistente.

**Próximo passo de maturidade**: para um cenário de dados verdadeiramente
incrementais (não um snapshot completo como o dataset atual), a evolução
natural seria adotar uma estratégia de **merge/upsert** (por exemplo, com
Delta Lake ou Apache Iceberg), fazendo *merge* por `Customer_ID` + `Month`
em vez de sobrescrever a tabela inteira — preservando histórico e reduzindo
o custo de reprocessamento em bases muito maiores.

---

## 4. Como organizar e armazenar os dados para facilitar consultas analíticas e alimentar dashboards ou modelos preditivos de classificação de crédito?

- **Formato Parquet**: colunar, comprimido e com schema embutido — muito
  mais eficiente que CSV para consultas analíticas (ferramentas como Athena,
  Redshift Spectrum ou Power BI leem apenas as colunas necessárias, não o
  arquivo inteiro).
- **Particionamento por `Credit_Score`**: como essa é a variável-alvo mais
  consultada pelas equipes de Analytics e Crédito (ex: "quero só os
  clientes classificados como `POOR` para o modelo de risco"), o
  particionamento físico permite *partition pruning* — a consulta ignora
  completamente os arquivos das outras classes, reduzindo custo e tempo de
  leitura.
- **Estrutura de camadas do Data Lake**: separação clara entre a camada
  *Raw* (CSV bruto, fiel à fonte) e a camada *Trusted* (Parquet limpo e
  validado) — times de Analytics e modelos de score consomem exclusivamente
  a camada Trusted, nunca o dado bruto.
- **Consumo por ferramentas de BI/ML**: a estrutura de pastas no S3
  (`credit_score_clean/Credit_Score=GOOD/...`) segue a convenção de
  particionamento estilo *Hive*, compatível nativamente com AWS Glue
  Catalog + Athena, e por consequência com Power BI (via conector S3/Athena)
  para dashboards — atendendo diretamente ao item de bônus do projeto.

---

*Documento elaborado como parte da documentação técnica do projeto final da
Trilha de Engenharia de Dados — Bootcamp [RE]Start, para a fintech fictícia
Data Girls Finance.*