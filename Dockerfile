FROM apache/airflow:2.9.0
# Install Java 17
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow