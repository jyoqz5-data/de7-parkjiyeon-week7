FROM apache/airflow:3.1.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -s "${JAVA_HOME_PATH}" /opt/java \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -r /requirements.txt