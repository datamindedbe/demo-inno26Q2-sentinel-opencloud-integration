FROM mcr.microsoft.com/vscode/devcontainers/python:3.11

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
