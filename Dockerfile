# Use the official Apache Airflow image as a base
FROM apache/airflow:2.7.2-python3.10

# Set Working Directory
WORKDIR /opt/airflow

# Copy DAGs and requirements file
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt

# Install Python dependencies as airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[all]  # Install DVC with optional S3 support

# Switch to root to allow modification of directories
USER root

RUN apt-get update && apt-get install -y \
    curl apt-transport-https ca-certificates gnupg

# Add Google Cloud SDK distribution URI as a package source
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee /etc/apt/sources.list.d/google-cloud-sdk.list

# Add the public key for the Google Cloud SDK repository
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

# Update package list and install Google Cloud SDK
RUN apt-get update && apt-get install -y google-cloud-sdk

# Clean up unnecessary files to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY ./sa-key.json /opt/airflow/sa-key.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/sa-key.json

# Install Google Cloud SDK
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk && \
    rm -rf /var/lib/apt/lists/*

# Install Docker
RUN apt-get update && apt-get install -y \
    docker.io && \
    rm -rf /var/lib/apt/lists/*

# Install kubectl
RUN apt-get update && apt-get install -y curl && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/

# Ensure entrypoint.sh is executable
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

# Switch back to airflow user for running Airflow processes
USER airflow
