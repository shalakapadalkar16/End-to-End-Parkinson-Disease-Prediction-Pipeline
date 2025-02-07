#!/bin/bash
set -e
echo "Authenticating with GCP..."
gcloud auth activate-service-account --key-file=/opt/airflow/sa-key.json

# Set GCP Project
gcloud config set project ${GCP_PROJECT_ID}
airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Mrudula --lastname Acharya --role Admin --email mrudulaacharya18@gmail.com --password smp1699

echo "Creating local directories..."
mkdir -p /opt/airflow/raw_data
mkdir -p /opt/airflow/motor_assessment
mkdir -p /opt/airflow/models
mkdir -p /opt/airflow/mlruns
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/outputs

if ! gsutil ls -b gs://${BUCKET_NAME}/; then
  echo "Bucket does not exist, creating it..."
  gsutil mb -p ${GCP_PROJECT_ID} -c STANDARD -l ${GCP_REGION} -b on gs://${BUCKET_NAME}
  echo "Granting Storage Object Admin role to ${SERVICE_ACCOUNT_EMAIL}..."
  gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT_EMAIL}:roles/storage.objectAdmin" "gs://${BUCKET_NAME}"
  gsutil
fi

# Check if the folders exist, and create them if they don't
for folder in models mlruns logs outputs ; do

    if ! gsutil ls -d gs://${BUCKET_NAME}/${folder}/ &>/dev/null; then
    echo "Folder gs://${BUCKET_NAME}/${folder}/ does not exist, creating it by adding an empty placeholder..."
    echo "Placeholder file for ${folder}" | gsutil cp - gs://${BUCKET_NAME}/${folder}/.placeholder
    fi
    
done



echo "Syncing data from GCP..."

# Sync raw data
gsutil -m rsync -r gs://${DATA_BUCKET_NAME}/raw_data /opt/airflow/raw_data
gsutil -m rsync -r gs://${DATA_BUCKET_NAME}/motor_assessment /opt/airflow/motor_assessment

# Sync mount folders
#gsutil rsync -r gs://your-bucket-name/outputs /opt/airflow/outputs
gsutil -m rsync -r gs://${BUCKET_NAME}/models /opt/airflow/models
gsutil -m rsync -r gs://${BUCKET_NAME}/mlruns /opt/airflow/mlruns
gsutil -m rsync -r gs://${BUCKET_NAME}/logs /opt/airflow/logs
gsutil -m rsync -r gs://${BUCKET_NAME}/outputs /opt/airflow/outputs

echo "Data sync complete!"



airflow scheduler &

sleep 60
# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver

airflow dags trigger data_pipeline
