# app/Dockerfile

FROM python:3-bookworm

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    ca-certificates \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install -r requirements.txt

COPY Docker-Admin/ForcepointCloudCA.crt /usr/local/share/ca-certificates/
COPY Docker-Admin/netskope-ca.crt /usr/local/share/ca-certificates/
COPY Docker-Admin/netskope-inter.crt /usr/local/share/ca-certificates/

RUN update-ca-certificates

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

EXPOSE 8601

HEALTHCHECK CMD curl --fail http://localhost:8601/_stcore/health

ENTRYPOINT ["streamlit", "run", "HF_CostOpt_Home.py", "--server.port=8601", "--server.address=0.0.0.0"]