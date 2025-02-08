# End-to-End-Parkinson-Disease-Prediction-Pipeline

## Project Overview
This project aims to predict Parkinson's disease using the **PPMI dataset** with a fully automated end-to-end machine learning pipeline deployed on **Google Cloud Platform (GCP)**. The pipeline includes data preprocessing, model training, inference, and deployment.

## Dataset
We utilized **8 tables** from the PPMI dataset, including:
- **5 motor assessment data tables**
- **Participant status and demographic information**
- **Biospecimen analysis**

The raw data is stored in **GCP Storage**, where it is retrieved, preprocessed, and used to train models.

---

## Architecture & Workflow
The workflow is implemented using **Airflow**, **Docker**, **MLflow**, and **Google Cloud Services**. Below is an overview of the architecture:

![Project Workflow](images/Parkinson disease prediction workflow.jpeg)

### **1. Data Pipeline (Airflow DAG)**
- Retrieves raw data files from **GCP Storage**.
- Preprocesses the data:
  - Removes **null** and **duplicate** values.
  - Formats data for model training.
- Saves the cleaned data back to **GCP Storage**.

### **2. Model Pipeline (Airflow DAG)**
- Loads cleaned data from **GCP Storage**.
- Trains four different models:
  - **Logistic Regression**
  - **Support Vector Machine (SVM)**
  - **Random Forest**
  - **XGBoost**
- Uses **MLflow** for experiment tracking and logging.
- Selects the best model based on performance metrics.
- Registers the chosen best model in **GCP Artifact Registry**.

### **3. Deployment**
- A **Flask web application** is built as a separate **Docker image**.
- The model is integrated into the Flask app for inference.
- The Docker image is stored in **GCP Storage**.
- The image is deployed on **Google Kubernetes Engine (GKE)**.
- A **service endpoint** is generated, allowing users to input patient data and receive predictions on whether they have Parkinson’s disease.

---

## Technologies & Tools Used
- **Airflow** – For orchestrating the data and model pipelines.
- **MLflow** – For experiment tracking and model logging.
- **Docker** – For containerizing both the data pipeline and model pipeline.
- **GCP Storage** – For storing raw and cleaned data, as well as Docker images.
- **GCP Artifact Registry** – For model versioning and storage.
- **Google Kubernetes Engine (GKE)** – For deploying the inference service.
- **Flask** – For serving predictions via a web app.

---

## How to Run the Project
### **1. Set Up GCP Storage and Artifact Registry**
- Upload raw data files to a **GCP Storage Bucket**.
- Set up **GCP Artifact Registry** for model versioning.

### **2. Run Data Pipeline**
- Execute the **data pipeline DAG** in **Airflow** to preprocess the data.

### **3. Train & Evaluate Models**
- Trigger the **model pipeline DAG** in **Airflow**.
- **MLflow** logs experiments and selects the best model.
- The best model is **registered in the GCP Artifact Registry**.

### **4. Deploy the Model**
- Build a **Docker image** containing the Flask app.
- Store the Docker image in **GCP Storage**.
- Deploy the image on **Google Kubernetes Engine (GKE)**.
- Retrieve the **service endpoint** and use it for inference.

---

## API Usage
### **Endpoint**
```
POST /predict
```
### **Request Format**
```json
{
  "feature1": value,
  "feature2": value,
  "feature3": value,
  ...
}
```
### **Response Format**
```json
{
  "prediction": "Positive/Negative"
}
```

---

## Repository Structure
```
├── dags/
│   ├── __pycache__/
│   ├── data_pipeline.py
│   ├── model_pipeline.py
├── docker_deploy/
│   ├── templates/
│   ├── Dockerfile
│   ├── app.py
│   ├── best_svm_model.pkl
│   ├── preprocessor.pkl
│   ├── requirements.txt
├── gcp_image/
│   ├── Dockerfile
│   ├── app.py
│   ├── model.pkl
│   ├── requirements.txt
├── logs/
│   ├── scheduler/
├── scripts/
├── tests/
```

---

## Future Improvements
- **Automate hyperparameter tuning** using **Optuna or Hyperopt**.
- **Improve model interpretability** using **SHAP or LIME**.
- **Enhance API security** with authentication.
- **Integrate monitoring** with **Prometheus and Grafana**.

---

## Contributors
- **Shalaka Padalkar** - [GitHub Profile](https://github.com/shalakapadalkar16)
- **Yutika Chougule**
- **Mrudula Acharya**

---

## License
This project is licensed under the MIT License - see the LICENSE file for details.

---

## Acknowledgments
- The **PPMI Dataset** was instrumental in developing this project.
- Special thanks to **Google Cloud Platform** for providing a robust infrastructure for deployment.

