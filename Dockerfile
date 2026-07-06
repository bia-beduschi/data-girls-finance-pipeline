# Estende a imagem oficial do Airflow adicionando Java (necessário para
# o PySpark rodar dentro do container) e as dependências do projeto.
FROM apache/airflow:2.9.3-python3.11

# Precisamos trocar para root temporariamente para instalar pacotes de sistema
USER root

# Instala o JDK (mesma necessidade que resolvemos no Windows, agora para o container Linux
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Define o JAVA_HOME para o PySpark encontrar o Java instalado
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Volta para o usuário airflow (nunca rode o processo principal como root)
USER airflow

# Copia e instala as dependências específicas do nosso pipeline
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt