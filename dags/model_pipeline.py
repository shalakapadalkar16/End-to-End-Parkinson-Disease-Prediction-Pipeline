import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from sklearn.model_selection import train_test_split as sklearn_train_test_split
import xgboost as xgb
import mlflow
import mlflow.sklearn
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import GridSearchCV
import joblib
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
import pickle
from fairlearn.metrics import MetricFrame, demographic_parity_difference, equalized_odds_difference
from sklearn.metrics import roc_curve, auc, confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt
from google.cloud import storage, aiplatform
from sklearn.preprocessing import label_binarize
import json
import logging
import numpy as np
import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO)

folder_path = '/opt/airflow/models'

# Set GCP Environment Variables (ensure these are set in docker-compose.yml or passed securely)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
DOCKER_IMAGE_NAME = os.getenv("DOCKER_IMAGE_NAME")
GKE_CLUSTER_NAME = os.getenv("GKE_CLUSTER_NAME")
GCP_ZONE = os.getenv("GCP_ZONE")
GKE_DEPLOYMENT_NAME = os.getenv("GKE_DEPLOYMENT_NAME")
GCP_ARTIFACT_REPO = os.getenv("GCP_ARTIFACT_REPO")
DEPLOYMENT_FILE = os.getenv("DEPLOYMENT_FILE")

# Set tracking server URI to a local directory.
mlflow.set_tracking_uri("file:/opt/airflow/mlruns")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': lambda context: send_custom_alert_email(
        "failed",
        task_id=context['task_instance'].task_id,
        dag_id=context['task_instance'].dag_id,
        **context)
}

dag = DAG(
    'model_pipeline',
    default_args=default_args,
    description='Model pipeline with custom email alerts for task failures and GCP integration',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Custom email alert function.
def send_custom_alert_email(status, task_id, dag_id, **context):
    subject = f"Airflow Task Alert - {status.capitalize()}"
    body = f"Task {task_id} in DAG {dag_id} has {status}."
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "mrudulaacharya18@gmail.com"
    msg['To'] = "mrudulaacharya18@gmail.com"

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("mrudulaacharya18@gmail.com", "lhwnkkhmvptmjghx")  # Use app-specific password
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            print(f"{status.capitalize()} email sent successfully.")
    except Exception as e:
        print(f"Error sending {status} email: {e}")



def get_data_from_data_pipeline(**context):
    try:
        logging.info("Load data...")
        mlflow.set_experiment("model_pipeline")
        df = context['dag_run'].conf['processed_data']
        context['ti'].xcom_push(key='df', value=df)
        logging.info("Data loaded successfully.")
        return df
    except Exception as e:
        logging.error("Error loading participant status data: %s", e)
        raise

def validate_data(**context):

    try:
        logging.info("Validating data")

        df=context['ti'].xcom_pull(key='df', task_ids='get_data_from_data_pipeline')
        
        df1 = pd.read_json(df, orient='split')
        # 1. Shape and Size Check
        if df1.shape[0] == 0 or df1.shape[1] == 0:
            raise ValueError("DataFrame is empty!")

        # 2. Column Existence Check.
        expected_columns = ['ENROLL_AGE', 'ENRLPRKN_0.0', 'ENRLPRKN_0.2', 'ENRLPRKN_1.0', 'ENRLSRDC_0.0', 'ENRLSRDC_0.2', 'ENRLSRDC_0.4', 'ENRLSRDC_0.6', 'ENRLSRDC_0.8', 'ENRLSRDC_1.0', 'ENRLHPSM_0.0', 'ENRLHPSM_1.0', 'ENRLRBD_0.0', 'ENRLRBD_1.0', 'ENRLSNCA_0.0', 'ENRLSNCA_1.0', 'ENRLGBA_0.0', 'ENRLGBA_1.0', 'SEX_0.0', 'SEX_1.0', 'NUPSOURC_1.0', 'NUPSOURC_2.0', 'NUPSOURC_3.0', 'NP1COG_0.0', 'NP1COG_1.0', 'NP1COG_2.0', 'NP1COG_3.0', 'NP1COG_4.0', 'NP1HALL_0.0', 'NP1HALL_1.0', 'NP1HALL_2.0', 'NP1HALL_3.0', 'NP1DPRS_0.0', 'NP1DPRS_1.0', 'NP1DPRS_2.0', 'NP1DPRS_3.0', 'NP1DPRS_4.0', 'NP1ANXS_0.0', 'NP1ANXS_1.0', 'NP1ANXS_2.0', 'NP1ANXS_3.0', 'NP1ANXS_4.0', 'NP1APAT_0.0', 'NP1APAT_1.0', 'NP1APAT_2.0', 'NP1APAT_3.0', 'NP1APAT_4.0', 'NP1DDS_0.0', 'NP1DDS_1.0', 'NP1DDS_2.0', 'NP1DDS_3.0', 'NP1DDS_101.0', 'NP1SLPN_0.0', 'NP1SLPN_1.0', 'NP1SLPN_2.0', 'NP1SLPN_3.0', 'NP1SLPN_4.0', 'NP1SLPD_0.0', 'NP1SLPD_1.0', 'NP1SLPD_2.0', 'NP1SLPD_3.0', 'NP1SLPD_4.0', 'NP1PAIN_0.0', 'NP1PAIN_1.0', 'NP1PAIN_2.0', 'NP1PAIN_3.0', 'NP1PAIN_4.0', 'NP1URIN_0.0', 'NP1URIN_1.0', 'NP1URIN_2.0', 'NP1URIN_3.0', 'NP1URIN_4.0', 'NP1CNST_0.0', 'NP1CNST_1.0', 'NP1CNST_2.0', 'NP1CNST_3.0', 'NP1CNST_4.0', 'NP1LTHD_0.0', 'NP1LTHD_1.0', 'NP1LTHD_2.0', 'NP1LTHD_3.0', 'NP1LTHD_4.0', 'NP1FATG_0.0', 'NP1FATG_1.0', 'NP1FATG_2.0', 'NP1FATG_3.0', 'NP1FATG_4.0', 'NP2SPCH_0.0', 'NP2SPCH_1.0', 'NP2SPCH_2.0', 'NP2SPCH_3.0', 'NP2SPCH_4.0', 'NP2SALV_0.0', 'NP2SALV_1.0', 'NP2SALV_2.0', 'NP2SALV_3.0', 'NP2SALV_4.0', 'NP2SWAL_0.0', 'NP2SWAL_1.0', 'NP2SWAL_2.0', 'NP2SWAL_3.0', 'NP2EAT_0.0', 'NP2EAT_1.0', 'NP2EAT_2.0', 'NP2EAT_3.0', 'NP2EAT_4.0', 'NP2DRES_0.0', 'NP2DRES_1.0', 'NP2DRES_2.0', 'NP2DRES_3.0', 'NP2HYGN_0.0', 'NP2HYGN_1.0', 'NP2HYGN_2.0', 'NP2HYGN_3.0', 'NP2HYGN_4.0', 'NP2HWRT_0.0', 'NP2HWRT_1.0', 'NP2HWRT_2.0', 'NP2HWRT_3.0', 'NP2HWRT_4.0', 'NP2HOBB_0.0', 'NP2HOBB_1.0', 'NP2HOBB_2.0', 'NP2HOBB_3.0', 'NP2HOBB_4.0', 'NP2TURN_0.0', 'NP2TURN_1.0', 'NP2TURN_2.0', 'NP2TURN_3.0', 'NP2TURN_4.0', 'NP2TRMR_0.0', 'NP2TRMR_1.0', 'NP2TRMR_2.0', 'NP2TRMR_3.0', 'NP2TRMR_4.0', 'NP2RISE_0.0', 'NP2RISE_1.0', 'NP2RISE_2.0', 'NP2RISE_3.0', 'NP2RISE_4.0', 'NP2WALK_0.0', 'NP2WALK_1.0', 'NP2WALK_2.0', 'NP2WALK_3.0', 'NP2WALK_4.0', 'NP2FREZ_0.0', 'NP2FREZ_1.0', 'NP2FREZ_2.0', 'NP2FREZ_3.0', 'NP2FREZ_4.0', 'NP3SPCH_0.0', 'NP3SPCH_1.0', 'NP3SPCH_2.0', 'NP3SPCH_3.0', 'NP3SPCH_4.0', 'NP3FACXP_0.0', 'NP3FACXP_1.0', 'NP3FACXP_2.0', 'NP3FACXP_3.0', 'NP3FACXP_4.0', 'NP3RIGN_0.0', 'NP3RIGN_1.0', 'NP3RIGN_2.0', 'NP3RIGN_3.0', 'NP3RIGN_4.0', 'NP3RIGN_101.0', 'NP3RIGRU_0.0', 'NP3RIGRU_1.0', 'NP3RIGRU_2.0', 'NP3RIGRU_3.0', 'NP3RIGRU_101.0', 'NP3RIGLU_0.0', 'NP3RIGLU_1.0', 'NP3RIGLU_2.0', 'NP3RIGLU_3.0', 'NP3RIGLU_4.0', 'NP3RIGLU_101.0', 'NP3RIGRL_0.0', 'NP3RIGRL_1.0', 'NP3RIGRL_2.0', 'NP3RIGRL_3.0', 'NP3RIGRL_101.0', 'NP3RIGLL_0.0', 'NP3RIGLL_1.0', 'NP3RIGLL_2.0', 'NP3RIGLL_3.0', 'NP3RIGLL_4.0', 'NP3RIGLL_101.0', 'NP3FTAPR_0.0', 'NP3FTAPR_1.0', 'NP3FTAPR_2.0', 'NP3FTAPR_3.0', 'NP3FTAPR_4.0', 'NP3FTAPR_101.0', 'NP3FTAPL_0.0', 'NP3FTAPL_1.0', 'NP3FTAPL_2.0', 'NP3FTAPL_3.0', 'NP3FTAPL_4.0', 'NP3FTAPL_101.0', 'NP3HMOVR_0.0', 'NP3HMOVR_1.0', 'NP3HMOVR_2.0', 'NP3HMOVR_3.0', 'NP3HMOVR_4.0', 'NP3HMOVR_101.0', 'NP3HMOVL_0.0', 'NP3HMOVL_1.0', 'NP3HMOVL_2.0', 'NP3HMOVL_3.0', 'NP3HMOVL_101.0', 'NP3PRSPR_0.0', 'NP3PRSPR_1.0', 'NP3PRSPR_2.0', 'NP3PRSPR_3.0', 'NP3PRSPL_0.0', 'NP3PRSPL_1.0', 'NP3PRSPL_2.0', 'NP3PRSPL_3.0', 'NP3PRSPL_4.0', 'NP3PRSPL_101.0', 'NP3TTAPR_0.0', 'NP3TTAPR_1.0', 'NP3TTAPR_2.0', 'NP3TTAPR_3.0', 'NP3TTAPR_4.0', 'NP3TTAPR_101.0', 'NP3TTAPL_0.0', 'NP3TTAPL_1.0', 'NP3TTAPL_2.0', 'NP3TTAPL_3.0', 'NP3TTAPL_4.0', 'NP3TTAPL_101.0', 'NP3LGAGR_0.0', 'NP3LGAGR_1.0', 'NP3LGAGR_2.0', 'NP3LGAGR_3.0', 'NP3LGAGR_101.0', 'NP3LGAGL_0.0', 'NP3LGAGL_1.0', 'NP3LGAGL_2.0', 'NP3LGAGL_3.0', 'NP3LGAGL_4.0', 'NP3LGAGL_101.0', 'NP3RISNG_0.0', 'NP3RISNG_1.0', 'NP3RISNG_2.0', 'NP3RISNG_3.0', 'NP3RISNG_101.0', 'NP3GAIT_0.0', 'NP3GAIT_1.0', 'NP3GAIT_2.0', 'NP3GAIT_3.0', 'NP3GAIT_4.0', 'NP3GAIT_101.0', 'NP3FRZGT_0.0', 'NP3FRZGT_1.0', 'NP3FRZGT_2.0', 'NP3FRZGT_3.0', 'NP3FRZGT_4.0', 'NP3FRZGT_101.0', 'NP3PSTBL_0.0', 'NP3PSTBL_1.0', 'NP3PSTBL_2.0', 'NP3PSTBL_3.0', 'NP3PSTBL_4.0', 'NP3PSTBL_101.0', 'NP3POSTR_0.0', 'NP3POSTR_1.0', 'NP3POSTR_2.0', 'NP3POSTR_3.0', 'NP3POSTR_4.0', 'NP3POSTR_101.0', 'NP3BRADY_0.0', 'NP3BRADY_1.0', 'NP3BRADY_2.0', 'NP3BRADY_3.0', 'NP3PTRMR_0.0', 'NP3PTRMR_1.0', 'NP3PTRMR_2.0', 'NP3PTRMR_3.0', 'NP3PTRMR_101.0', 'NP3PTRML_0.0', 'NP3PTRML_1.0', 'NP3PTRML_2.0', 'NP3PTRML_3.0', 'NP3KTRMR_0.0', 'NP3KTRMR_1.0', 'NP3KTRMR_2.0', 'NP3KTRMR_3.0', 'NP3KTRMR_101.0', 'NP3KTRML_0.0', 'NP3KTRML_1.0', 'NP3KTRML_2.0', 'NP3KTRML_3.0', 'NP3KTRML_101.0', 'NP3RTARU_0.0', 'NP3RTARU_1.0', 'NP3RTARU_2.0', 'NP3RTARU_3.0', 'NP3RTARU_4.0', 'NP3RTALU_0.0', 'NP3RTALU_1.0', 'NP3RTALU_2.0', 'NP3RTALU_3.0', 'NP3RTALU_4.0', 'NP3RTARL_0.0', 'NP3RTARL_1.0', 'NP3RTARL_2.0', 'NP3RTARL_3.0', 'NP3RTARL_4.0', 'NP3RTALL_0.0', 'NP3RTALL_1.0', 'NP3RTALL_2.0', 'NP3RTALL_3.0', 'NP3RTALJ_0.0', 'NP3RTALJ_1.0', 'NP3RTALJ_2.0', 'NP3RTALJ_4.0', 'NP3RTALJ_101.0', 'NP3RTCON_0.0', 'NP3RTCON_1.0', 'NP3RTCON_2.0', 'NP3RTCON_3.0', 'NP3RTCON_4.0', 'NP3RTCON_101.0', 'DYSKPRES_0.0', 'DYSKPRES_1.0', 'NHY_0.0', 'NHY_1.0', 'NHY_2.0', 'NHY_3.0', 'NHY_4.0', 'NHY_101.0', 'COHORT']  # Update with your actual expected columns
        if not all(col in df1.columns for col in expected_columns):
            raise ValueError(f"Missing columns: {set(expected_columns) - set(df1.columns)}")

        # 3. Null Values Check
        if df1.isnull().any().any():
            raise ValueError("Data contains missing values!")
        #context['ti'].xcom_push(key='df', value=df).
        logging.info("Data validated!")
        return True

    except Exception as e:
        logging.error("Error loading demographics data: %s", e)
        raise  
    
    
def split_to_X_y(**context):

    try:
        logging.info("Split data target")
        df=context['ti'].xcom_pull(key='df', task_ids='get_data_from_data_pipeline')
        df = pd.read_json(df, orient='split')
        logging.info(f"Retrieved df from XCom: {df}")
        y=df['COHORT']
        y = LabelEncoder().fit_transform(y)
        X=df.drop(columns=['COHORT'])
        context['ti'].xcom_push(key='y', value=y.tolist())
        context['ti'].xcom_push(key='X', value=X)
        logging.info("Split succesful")
        return X,y.tolist()
    except Exception as e:
        logging.error("Error loading demographics data: %s", e)
        raise
    

def train_test_split(**context):

    try:

        logging.info("Train test split")
        X= context['ti'].xcom_pull(key='X', task_ids='split_to_X_y')
        y= context['ti'].xcom_pull(key='y', task_ids='split_to_X_y')
        # Split data into train/test
        X_train, X_temp, y_train, y_temp = sklearn_train_test_split(X, y, test_size=0.30, random_state=42)
        X_val, X_test, y_val, y_test = sklearn_train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

        context['ti'].xcom_push(key='y_train', value=y_train)
        context['ti'].xcom_push(key='X_train', value=X_train)
        context['ti'].xcom_push(key='y_val', value=y_val)
        context['ti'].xcom_push(key='X_val', value=X_val)
        context['ti'].xcom_push(key='y_test', value=y_test)
        context['ti'].xcom_push(key='X_test', value=X_test)
        logging.info("Split successful")
        return X_train,y_train,X_val,y_val,X_test,y_test
    
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise



def train_and_tune_model(model, param_grid, model_name, X_train, y_train, X_val, y_val, **context):
    try:    
        # Set tracking URI and experiment
        logging.info("Training Model")
        mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
        mlflow.set_experiment("Model_Training_Tuning")
        with mlflow.start_run(run_name=f"{model_name}_Tuning"):
            grid_search = GridSearchCV(model, param_grid=param_grid, cv=3, scoring='accuracy', n_jobs=-1)
            grid_search.fit(X_train, y_train)
            
            best_model = grid_search.best_estimator_
            best_params = grid_search.best_params_
            val_accuracy = best_model.score(X_val, y_val)
            
            # Log best parameters and metrics
            mlflow.log_params(best_params)
            mlflow.log_metric("validation_accuracy", val_accuracy)
            mlflow.sklearn.log_model(best_model, f"{model_name}_Tuned_Model")
            
            print(f"{model_name} Best Params: {best_params}, Validation Accuracy: {val_accuracy}")
            return best_model
        logging.info("Training Done")
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise
    
def evaluate_on_test_set(model, X_test, y_test):

    try:

        logging.info("Test Model")
        # Set tracking URI and experiment tracking.
        mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
        mlflow.set_experiment("Model_Testing")

        # Convert y_test to binary format for multiclass (One-vs-Rest strategy)
        unique_classes = sorted(list(set(y_test)))
        y_test_binary = label_binarize(y_test, classes=unique_classes)
        n_classes = len(unique_classes)

        with mlflow.start_run(run_name=f"Test_{model.__class__.__name__}"):
            y_pred = model.predict(X_test)

            # Calculate test accuracy and log it
            test_accuracy = accuracy_score(y_test, y_pred)
            mlflow.log_metric("test_accuracy", test_accuracy)

            # Calculate ROC-AUC scores for each class (One-vs-Rest)
            if hasattr(model, "predict_proba"):
                y_score = model.predict_proba(X_test)
                roc_auc_scores = {}

                for i, class_label in enumerate(unique_classes):
                    fpr, tpr, _ = roc_curve(y_test_binary[:, i], y_score[:, i])
                    roc_auc = auc(fpr, tpr)
                    roc_auc_scores[class_label] = roc_auc

                    # Plot and save the ROC curve
                    plt.figure()
                    plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (area = {roc_auc:.2f})')
                    plt.legend(loc="lower right")
                    plt.title(f"ROC Curve for Class {class_label}")
                    plt.savefig(f"/opt/airflow/outputs/roc_curve_class_{class_label}.png")
                    mlflow.log_artifact(f"/opt/airflow/outputs/roc_curve_class_{class_label}.png")

                # Log average AUC across all classes
                avg_roc_auc = np.mean(list(roc_auc_scores.values()))
                mlflow.log_metric("avg_roc_auc", avg_roc_auc)
            else:
                print("Model does not support predict_proba. Skipping ROC-AUC calculation.")

            # Log classification report as text for easy access
            report_text = classification_report(y_test, y_pred)
            print(f"Test Accuracy for {model.__class__.__name__}: {test_accuracy}")
            print(f"Classification Report:\n{report_text}")

            mlflow.log_metrics({
                "accuracy": test_accuracy,
            })
            mlflow.log_text(report_text, "classification_report.txt")

            # Confusion Matrix
            cm = confusion_matrix(y_test, y_pred)
            ConfusionMatrixDisplay(cm, display_labels=unique_classes).plot()
            plt.savefig("/opt/airflow/outputs/confusion_matrix.png")
            mlflow.log_artifact("/opt/airflow/outputs/confusion_matrix.png")
            logging.info("Testing Done")

            return test_accuracy, report_text
    except Exception as e:
        logging.error("Error loading demographics data: %s", e)
        raise
    
    
    

# Define functions that wrap the training and tuning function for each model
def tune_SVM(**context):

    try:
        logging.info("Tune SVM")
        param_grid_svm = {
            'C': [0.1, 1, 10],
            'kernel': ['linear', 'rbf'],
            'gamma': ['scale', 'auto']
        }
        svm_model = SVC(random_state=42)
        best_model_SVM= train_and_tune_model(svm_model, param_grid_svm, "SVM", 
                                    context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                    **context)
        
        # Check if the folder exists
        if not os.path.exists(folder_path):
            # Create the folder if it doesn't exist
            os.makedirs(folder_path)

        svm_model_filename = f"/opt/airflow/models/best_svm_model.pkl"
        joblib.dump(best_model_SVM, svm_model_filename)
        context['ti'].xcom_push(key=f"best_svm_model", value=svm_model_filename)
        logging.info("SVM Done")

    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

    
def tune_RF(**context):

    try:

        logging.info("Tune RF")
        param_grid_rf = {
            'n_estimators': [50, 100, 200],
            'max_depth': [10, 20, 30],
            'min_samples_split': [2, 5],
            'min_samples_leaf': [1, 2]
        }
        rf_model = RandomForestClassifier(random_state=42)
        best_model_rf= train_and_tune_model(rf_model, param_grid_rf, "RF", 
                                    context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                    **context)
        rf_model_filename = f"/opt/airflow/models/best_rf_model.pkl"
        joblib.dump(best_model_rf, rf_model_filename)
        context['ti'].xcom_push(key=f"best_rf_model", value=rf_model_filename)
        logging.info("RF done")
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise
    

def tune_XGB(**context):

    try:

        logging.info("Tune XGB")
        param_grid_xgb = {
            'max_depth': [3, 6, 10],
            'learning_rate': [0.01, 0.1, 0.3],
            'n_estimators': [50, 100, 200],
            'subsample': [0.8, 0.9, 1.0]
        }
        xgb_model = xgb.XGBClassifier(random_state=42)
        best_model_xgb= train_and_tune_model(xgb_model, param_grid_xgb, "XGB", 
                                    context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                    **context)
        
        xgb_model_filename = f"/opt/airflow/models/best_xgb_model.pkl"
        joblib.dump(best_model_xgb, xgb_model_filename)
        context['ti'].xcom_push(key=f"best_xgb_model", value=xgb_model_filename)
        logging.info("XGB done")
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise
    
def tune_LR(**context):
    try:

        logging.info("Tune LR")
        param_grid_lr = {
            'C': [0.1, 1, 10],
            'solver': ['liblinear', 'saga'],
            'penalty': ['l2']
        }
        lr_model = LogisticRegression(random_state=42)
        best_model_lr= train_and_tune_model(lr_model, param_grid_lr, "LR", 
                                    context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                    context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                    **context)
        lr_model_filename = f"/opt/airflow/models/best_lr_model.pkl"
        joblib.dump(best_model_lr, lr_model_filename)
        context['ti'].xcom_push(key=f"best_lr_model", value=lr_model_filename)
        logging.info("LR done")

    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise
   
def model_accuracies(**context):

    try:
        # Set the tracking URI for MLflow

        logging.info("Model Accuracies Model")
        mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
        
        # Define the experiment name
        experiment_name = "Model_Validation_Accuracies"
        
        # Ensure the experiment exists
        if not mlflow.get_experiment_by_name(experiment_name):
            mlflow.create_experiment(experiment_name)
        
        # Set the experiment to log runs
        mlflow.set_experiment(experiment_name)
        
        # Retrieve validation accuracies and models from XCom
        svm_model_path = context['ti'].xcom_pull(key='best_svm_model', task_ids='tune_SVM')
        svm = joblib.load(svm_model_path)

        rf_model_path = context['ti'].xcom_pull(key='best_rf_model', task_ids='tune_RF')
        rf = joblib.load(rf_model_path)

        xgb_model_path = context['ti'].xcom_pull(key='best_xgb_model', task_ids='tune_XGB')
        xgb = joblib.load(xgb_model_path)

        lr_model_path = context['ti'].xcom_pull(key='best_lr_model', task_ids='tune_LR')
        lr = joblib.load(lr_model_path)
        
        # Retrieve validation data
        X_val = context['ti'].xcom_pull(key='X_val', task_ids='train_test_split')
        y_val = context['ti'].xcom_pull(key='y_val', task_ids='train_test_split')

        # Calculate validation accuracies
        svm_accuracy = svm.score(X_val, y_val)
        rf_accuracy = rf.score(X_val, y_val)
        xgb_accuracy = xgb.score(X_val, y_val)
        lr_accuracy = lr.score(X_val, y_val)

        # Store models and accuracies in a dictionary
        model_accuracies = {
            'SVM': (svm_model_path, svm_accuracy),
            'Random Forest': (rf_model_path, rf_accuracy),
            'XGBoost': (xgb_model_path, xgb_accuracy),
            'Logistic Regression': (lr_model_path, lr_accuracy)
        }

        # Start an MLflow run
        with mlflow.start_run(run_name="Model_Validation_Accuracies"):
            for model_name, (model_path, accuracy) in model_accuracies.items():
                # Log the accuracy metric
                mlflow.log_metric(f"{model_name}_accuracy", accuracy)
                
                # Log the model file as an artifact
                mlflow.log_artifact(model_path, artifact_path=f"models/{model_name}")

        # Push the model accuracies to XCom
        context['ti'].xcom_push(key='model_accuracies', value=model_accuracies)
        logging.info("Model accuracies calculated")
        return model_accuracies
    
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

def bias_report(**context):

    try:
        logging.info("Generating bias report")
        # Set tracking URI and experiment
        mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
        mlflow.set_experiment("Model_Fairness_Evaluation")
        """
        Compute fairness metrics for multiple models and log results using MLflow.

        Parameters:
        - X_val: DataFrame, validation feature set (one-hot encoded for categorical variables like SEX).
        - y_val: Series or ndarray, validation target set.
        - models: Dictionary of {model_name: model_object}.
        - sensitive_feature_columns: List of columns in X_val used as sensitive features for fairness evaluation.
        - mlflow_logger: MLflow logging object.
        
        Returns:
        - fairness_report: Dictionary containing fairness metrics for all models.

        """
        # Retrieve models from XCom
        models = {
            'SVM': context['ti'].xcom_pull(key='best_svm_model', task_ids='tune_SVM'),
            'Random Forest': context['ti'].xcom_pull(key='best_rf_model', task_ids='tune_RF'),
            'XGBoost': context['ti'].xcom_pull(key='best_xgb_model', task_ids='tune_XGB'),
            'Logistic Regression': context['ti'].xcom_pull(key='best_lr_model', task_ids='tune_LR'),
        }

        # Load models
        loaded_models = {name: joblib.load(model) for name, model in models.items()}

        # Retrieve validation data and labels
        X_val = context['ti'].xcom_pull(key='X_val', task_ids='train_test_split')
        y_val = context['ti'].xcom_pull(key='y_val', task_ids='train_test_split')
        y_val = pd.Series(y_val, index=X_val.index)
        
        # Initialize a list to hold the results
        model_results = []
        
        # Define sensitive feature columns
        sensitive_feature_columns = ['SEX_0.0', 'SEX_1.0', 'ENROLL_AGE']
        sensitive_features = X_val[sensitive_feature_columns].copy()

        # Handle ENROLL_AGE_BINNED creation
        max_value = X_val["ENROLL_AGE"].max()
        min_value = X_val["ENROLL_AGE"].min()
        num_bins = 4
        sensitive_features["ENROLL_AGE_BINNED"] = pd.cut(
            X_val["ENROLL_AGE"],
            bins=np.linspace(min_value, max_value, num_bins + 1),
            labels=[f"Bin {i}" for i in range(1, num_bins + 1)],
            right=False
        ).astype(str)

        # Evaluate fairness for each model
        for model_name, model in loaded_models.items():
            print(f"Evaluating fairness for model: {model_name}")
            mlflow.start_run(run_name=f"Fairness Evaluation - {model_name}")
            
            # Predict on validation data
            y_pred = model.predict(X_val)

            for class_label in np.unique(y_val):
                print(f"Analyzing fairness for class: {class_label}")
                
                # Binarize the y_val and y_pred for the current class
                y_val_binary = (y_val == class_label).astype(int)
                y_pred_binary = (y_pred == class_label).astype(int)

            # Loop through sensitive features and calculate fairness metrics
                for feature in sensitive_features.columns:
                    print(f"Analyzing fairness for sensitive feature: {feature}")
                    
                    # Get the sensitive feature values for the current feature
                    feature_sensitive_values = sensitive_features[feature]

                    # Demographic Parity Difference
                    dp_diff = demographic_parity_difference(
                        y_val_binary,
                        y_pred_binary,
                        sensitive_features=feature_sensitive_values
                    )

                    # Equalized Odds Difference (if required)
                    eo_diff = equalized_odds_difference(
                        y_val_binary,
                        y_pred_binary,
                        sensitive_features=feature_sensitive_values
                    )
                    accuracy = (y_pred_binary == y_val_binary).mean()


                    # Append results for each bin
                    if feature == 'ENROLL_AGE_BINNED':
                        for age_bin in sensitive_features[feature].unique():
                            accuracy = (y_pred == y_val).mean()  # Calculate accuracy
                            model_results.append({
                                'model': model_name,
                                'slice': f"AGE_{age_bin}",
                                'accuracy': accuracy,
                                'dp_diff': dp_diff,
                                'eo_diff': eo_diff
                            })
                    else:
                        # For non-age features, calculate accuracy and log fairness metrics
                        accuracy = (y_pred == y_val).mean()  # Calculate accuracy
                        model_results.append({
                            'model': model_name,
                            'slice': f"{feature}",
                            'accuracy': accuracy,
                            'dp_diff': dp_diff,
                            'eo_diff': eo_diff
                        })
            mlflow.log_dict(model_results, "fairness_metrics.json")


            mlflow.end_run()

        # Log the model results
        context['ti'].xcom_push(key='bias_report', value=model_results)
        logging.info("Bias report done")
        return model_results
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

def model_ranking(**context):

    try:
        logging.info("Model Ranking")
        fairness_report = context['ti'].xcom_pull(key='bias_report', task_ids='bias_report')
        model_accuracies = context['ti'].xcom_pull(key='model_accuracies', task_ids='model_accuracies')

        if not fairness_report:
            raise ValueError("Fairness report not found or empty in XCom.")
        if not model_accuracies:
            raise ValueError("Model accuracies not found or empty in XCom.")

        model_fairness_penalties = {}

        for entry in fairness_report:
            model_name = entry['model']
            dp_diff = entry['dp_diff']
            eo_diff = entry['eo_diff']

            if model_name not in model_fairness_penalties:
                model_fairness_penalties[model_name] = []

            model_fairness_penalties[model_name].append(abs(dp_diff) + abs(eo_diff))

        for model_name in model_fairness_penalties:
            penalties = model_fairness_penalties[model_name]
            model_fairness_penalties[model_name] = np.mean(penalties)

        ranked_models = []
        for model_name, (model_object, accuracy) in model_accuracies.items():
            fairness_penalty = model_fairness_penalties.get(model_name, 0)
            score = accuracy - 0.7 * fairness_penalty

            # Enhanced model type mapping
            model_type = None
            if "XGB" in model_name.upper() or "BOOST" in model_name.upper():
                model_type = "xgboost"
            elif "RF" in model_name.upper() or "FOREST" in model_name.upper():
                model_type = "random_forest"
            elif "LR" in model_name.upper() or "LOGISTIC" in model_name.upper():
                model_type = "logistic_regression"
            elif "SVM" in model_name.upper():
                model_type = "svm"
            else:
                raise ValueError(f"Unrecognized model type for model_name: {model_name}")

            ranked_models.append({
                "model_name": model_name,
                "model_object": model_object,
                "score": score,
                "model_type": model_type,
            })

        ranked_models = sorted(ranked_models, key=lambda x: x['score'], reverse=True)
        context['ti'].xcom_push(key='model_ranking', value=ranked_models)
        logging.info("Models ranked")
        return ranked_models
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

def select_best_model(**context):
    # Retrieve the ranked models from XCom

    """Select the best model based on ranking and push its details to XCom."""
    # Retrieve the ranked models from XCom
    try:
        logging.info("Best model selection")
        model_ranking = context['ti'].xcom_pull(key='model_ranking', task_ids='model_ranking')

        # Validate that the model ranking exists
        if not model_ranking or len(model_ranking) == 0:
            raise ValueError("Model ranking is empty or not found in XCom.")

        # Select the best model (first entry in the ranking list)
        best_model = model_ranking[0]
        best_model_name = best_model["model_name"]
        best_model_object = best_model["model_object"]
        best_model_score = best_model["score"]
        best_model_type = best_model["model_type"]

        # Log the details of the best model for debugging
        logging.info(f"Best Model Selected: Name: {best_model_name}, Score: {best_model_score}, Type: {best_model_type}")

        # Push the best model details to XCom for downstream tasks
        context['ti'].xcom_push(key='best_model', value=best_model_object)
        context['ti'].xcom_push(key='best_model_name', value=best_model_name)
        context['ti'].xcom_push(key='best_model_type', value=best_model_type)
        context['ti'].xcom_push(key='best_model_path', value=f"/opt/airflow/models/{best_model_name.lower().replace(' ', '_')}.pkl")

        return best_model_object
    
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

def test_best_model(**context):
    # Pull the best model path from XCom
    try:
        logging.info("Test best model")
        best_model = context['ti'].xcom_pull(key="best_model", task_ids='select_best_model')

        if not best_model:
            raise ValueError("Best model path is None. Ensure 'select_best_model' task ran successfully and pushed the model path to XCom.")

        logging.info(f"Best model path retrieved: {best_model}")
        model = joblib.load(best_model)  # Load the model

        # Retrieve test data
        X_test = context['ti'].xcom_pull(key='X_test', task_ids='train_test_split')
        y_test = context['ti'].xcom_pull(key='y_test', task_ids='train_test_split')

        if X_test is None or y_test is None:
            raise ValueError("Test data (X_test or y_test) not found in XCom.")

        # Evaluate the model
        test_accuracy, report_text = evaluate_on_test_set(model, X_test, y_test)

        logging.info(f"Test Accuracy for the best model: {test_accuracy}")
        logging.info(f"Classification Report:\n{report_text}")

        return test_accuracy, report_text
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

def register_best_model(**context):
    # Set tracking URI and experiment
    try:
        logging.info("Register MOdel")
        mlflow.set_tracking_uri("file:/opt/airflow/mlruns")
        mlflow.set_experiment("Best_model_registeration")

        # Pull the best model and model name from XCom
        best_model = context['ti'].xcom_pull(key='best_model', task_ids='select_best_model')
        best_model_name = context['ti'].xcom_pull(key='best_model_name', task_ids='select_best_model')
        
        # Start MLflow run for model registration
        with mlflow.start_run(run_name="Model_Registration"):
            # Log the model
            model_path = f"models/{best_model_name}"
            mlflow.sklearn.log_model(best_model, model_path)
            
            # Register the model to the model registry
            model_uri = f"runs:/{mlflow.active_run().info.run_id}/{model_path}"
            mlflow.register_model(model_uri, best_model_name)
            
        print(f"Registered {best_model_name} as the best model.")
    except Exception as e:
            logging.error("Error loading demographics data: %s", e)
            raise

get_data_from_data_pipeline_task = PythonOperator(
    task_id='get_data_from_data_pipeline',
    python_callable= get_data_from_data_pipeline,
    provide_context=True,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable= validate_data,
    provide_context=True,
    dag=dag,
)

split_to_X_y_task = PythonOperator(
    task_id='split_to_X_y',
    python_callable=split_to_X_y,
    provide_context=True,
    dag=dag,
)

train_test_split_task = PythonOperator(
    task_id='train_test_split',
    python_callable=train_test_split,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: send_custom_alert_email(
            "succeeded", 
            task_id=context['task_instance'].task_id, 
            dag_id=context['task_instance'].dag_id, 
            **context
        )
)

tune_SVM_task = PythonOperator(
    task_id='tune_SVM',
    python_callable=tune_SVM,
    provide_context=True,
    dag=dag,
)

tune_LR_task = PythonOperator(
    task_id='tune_LR',
    python_callable=tune_LR,
    provide_context=True,
    dag=dag,
)
tune_RF_task = PythonOperator(
    task_id='tune_RF',
    python_callable=tune_RF,
    provide_context=True,
    dag=dag,
)
tune_XGB_task = PythonOperator(
    task_id='tune_XGB',
    python_callable=tune_XGB,
    provide_context=True,
    dag=dag,
)

model_accuracies_task = PythonOperator(
    task_id='model_accuracies',
    python_callable=model_accuracies,
    provide_context=True,
    dag=dag,
)

bias_report_task = PythonOperator(
    task_id='bias_report',
    python_callable=bias_report,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: send_custom_alert_email(
            "succeeded", 
            task_id=context['task_instance'].task_id, 
            dag_id=context['task_instance'].dag_id, 
            **context
        )
)

model_ranking_task = PythonOperator(
    task_id='model_ranking',
    python_callable=model_ranking,
    provide_context=True,
    dag=dag,
)

select_best_model_task = PythonOperator(
    task_id='select_best_model',
    python_callable=select_best_model,
    provide_context=True,
    dag=dag,
)
test_best_model_task = PythonOperator(
    task_id='test_best_model',
    python_callable=test_best_model,
    provide_context=True,
    dag=dag,
)

register_best_model_task=PythonOperator(
    task_id='register_best_model',
    python_callable=register_best_model,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: send_custom_alert_email(
            "succeeded", 
            task_id=context['task_instance'].task_id, 
            dag_id=context['task_instance'].dag_id, 
            **context
        )
)

# Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

prepare_image_folder_task = BashOperator(
    task_id='prepare_image_folder',
    bash_command="""
    mkdir -p /opt/airflow/gcp_image && \
    cp {{ ti.xcom_pull(task_ids='select_best_model', key='best_model') }} /opt/airflow/gcp_image/model.pkl
    """,
    dag=dag,
)


build_docker_image_task = BashOperator(
    task_id='build_docker_image',
    bash_command = """
    export DOCKER_HOST=unix:///var/run/docker.sock &&
    docker build \
    -t ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GCP_ARTIFACT_REPO}/${DOCKER_IMAGE_NAME}:latest \
    /opt/airflow/gcp_image""",
    dag=dag,
    env={
        'GCP_REGION': GCP_REGION,
        'GCP_ARTIFACT_REPO': GCP_ARTIFACT_REPO,
        'GCP_PROJECT_ID': GCP_PROJECT_ID,
        'DOCKER_IMAGE_NAME': DOCKER_IMAGE_NAME,
    },
)

push_image_to_artifact_registry_task = BashOperator(
    task_id='push_image_to_artifact_registry',
    bash_command="""
    # Authenticate the service account
    gcloud auth activate-service-account --key-file=/opt/airflow/sa-key.json &&
    
    # Set the GCP project
    gcloud config set project ${GCP_PROJECT_ID} &&
    
    # Create Artifact Registry repository (skip if it already exists)
    gcloud artifacts repositories create ${GCP_ARTIFACT_REPO} \
        --repository-format=docker \
        --location=${GCP_REGION} \
        --description="Docker repository" \
        --project=${GCP_PROJECT_ID} \
        --quiet || true &&

    # Configure Docker to use gcloud as a credential helper
    gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://${GCP_REGION}-docker.pkg.dev &&

    # Push the Docker image to Artifact Registry
    docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GCP_ARTIFACT_REPO}/${DOCKER_IMAGE_NAME}:latest
    """,
    dag=dag,
    env={
        'GCP_PROJECT_ID': GCP_PROJECT_ID,     # Ensure this is defined in your DAG
        'GCP_REGION': GCP_REGION,             # Ensure this is defined in your DAG
        'DOCKER_IMAGE_NAME': DOCKER_IMAGE_NAME,  # Replace with the image name
        'GCP_ARTIFACT_REPO': GCP_ARTIFACT_REPO,  # Replace with the repository name
    },
)

# push_image_to_artifact_registry_task = BashOperator(
#     task_id='push_image_to_artifact_registry',
#     bash_command="""
#     # Authenticate the service account
#     gcloud auth activate-service-account --key-file=/opt/airflow/sa-key.json &&
    
#     # Set the GCP project
#     gcloud config set project ${GCP_PROJECT_ID} &&
    
#     # Check if the Artifact Registry repository exists
#     if gcloud artifacts repositories list --location=${GCP_REGION} \
#        --filter="name:${GCP_ARTIFACT_REPO}" --format="value(name)" | grep -q ${GCP_ARTIFACT_REPO}; then
#         echo "Artifact Registry repository ${GCP_ARTIFACT_REPO} already exists."
#     else
#         echo "Creating Artifact Registry repository ${GCP_ARTIFACT_REPO}..."
#         gcloud artifacts repositories create ${GCP_ARTIFACT_REPO} \
#             --repository-format=docker \
#             --location=${GCP_REGION} \
#             --description="Docker repository" \
#             --project=${GCP_PROJECT_ID} \
#             --quiet
#     fi &&
    
#     # Configure Docker to use gcloud as a credential helper
#     gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://${GCP_REGION}-docker.pkg.dev &&
    
#     # Push the Docker image to Artifact Registry
#     docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GCP_ARTIFACT_REPO}/${DOCKER_IMAGE_NAME}:latest
#     """,
#     dag=dag,
#     env={
#         'GCP_PROJECT_ID': GCP_PROJECT_ID,        # Ensure this is defined in your DAG
#         'GCP_REGION': GCP_REGION,               # Ensure this is defined in your DAG
#         'DOCKER_IMAGE_NAME': DOCKER_IMAGE_NAME, # Replace with the image name
#         'GCP_ARTIFACT_REPO': GCP_ARTIFACT_REPO, # Replace with the repository name
#     },
# )


deploy_to_gke_task = BashOperator(
    task_id='deploy_to_gke',
    bash_command="""
    # 1. Create the GKE Cluster
    echo "Creating GKE cluster: $GKE_CLUSTER_NAME"
    gcloud container clusters create-auto ${GKE_CLUSTER_NAME} --zone=${GCP_ZONE} --project=${GCP_PROJECT_ID}

    # 2. Obtain authentication credentials for kubectl
    echo "Fetching GKE credentials for cluster: $GKE_CLUSTER_NAME"
    gcloud container clusters get-credentials ${GKE_CLUSTER_NAME} --zone=${GCP_ZONE} --project=${GCP_PROJECT_ID}

    # 3. 
    kubectl create secret docker-registry artifact-registry-credentials \
    --docker-server=${GCP_REGION}-docker.pkg.dev \
    --docker-username=_json_key \
    --docker-password="$(cat /opt/airflow/sa-key.json)" \
    --docker-email=shalakapadalkar16@gmail.com


    # 4. Verify authentication and connectivity
    echo "Verifying connectivity to the GKE cluster"
    kubectl get nodes

    # 5. Deploy the Docker image using the deployment.yaml file
    echo "Deploying application using $DEPLOYMENT_FILE"
    kubectl apply -f ${DEPLOYMENT_FILE}

    # 6. Expose the deployment with a LoadBalancer
    echo "Exposing deployment as a LoadBalancer"
    kubectl expose deployment model-deployment --type=LoadBalancer --port=80 --target-port=8080

    # 7. Fetch the external IP of the LoadBalancer
    echo "Fetching external IP of the LoadBalancer"
    EXTERNAL_IP=$(kubectl get service model-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

    # Wait for the LoadBalancer IP to become available
    while [ -z "$EXTERNAL_IP" ]; do
        echo "Waiting for LoadBalancer IP..."
        sleep 10
        EXTERNAL_IP=$(kubectl get service model-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    done

    echo "Application is accessible at: http://$EXTERNAL_IP"
    """,
    dag=dag,
    env={
        'GCP_PROJECT_ID': GCP_PROJECT_ID,
        'GCP_REGION': GCP_REGION,
        'GCP_ZONE': GCP_ZONE,              # Replace with your zone
        'GKE_CLUSTER_NAME': GKE_CLUSTER_NAME,      # Replace with your cluster name
        'DOCKER_IMAGE': "${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GCP_ARTIFACT_REPO}/${DOCKER_IMAGE_NAME}:latest",
        'GCP_ARTIFACT_REPO': GCP_ARTIFACT_REPO,
        'GKE_DEPLOYMENT_NAME': GKE_DEPLOYMENT_NAME,
        "DEPLOYMENT_FILE": DEPLOYMENT_FILE,
    },
)

test_inference_task = BashOperator(
    task_id='test_inference',
    bash_command="""
    # Wait for the external IP to be assigned
    external_ip=""
    while [ -z "$external_ip" ]; do
      echo "Waiting for end point..."
      external_ip=$(kubectl get svc ${GKE_DEPLOYMENT_NAME} --output=jsonpath='{.status.loadBalancer.ingress[0].ip}')
      [ -z "$external_ip" ] && sleep 10
    done
    echo "Endpoint ready: $external_ip"

    # Test inference
    curl http://$external_ip/predict
    """,
    dag=dag,

    env={
        'GKE_DEPLOYMENT_NAME': GKE_DEPLOYMENT_NAME,
    },
)
sync_logs = BashOperator(
        task_id='sync_logs',
        bash_command='gsutil -m rsync -r -x ".*scheduler/latest" /opt/airflow/logs gs://parkinsons_prediction_data_bucket/logs',
        trigger_rule='all_done'
    )
    
sync_outputs = BashOperator(
        task_id='sync_outputs',
        bash_command='gsutil -m  rsync -r /opt/airflow/outputs gs://parkinsons_prediction_data_bucket/outputs',
        trigger_rule='all_done'
    )
    
sync_mlrun = BashOperator(
        task_id='sync_mlrun',
        bash_command='gsutil -m  rsync -r /opt/airflow/mlruns gs://parkinsons_prediction_data_bucket/mlruns',
        trigger_rule='all_done'
    )
sync_models= BashOperator(
        task_id='sync_models',
        bash_command='gsutil -m  rsync -r /opt/airflow/models gs://parkinsons_prediction_data_bucket/models',
        trigger_rule='all_done'
    )

get_data_from_data_pipeline_task>>validate_data_task>>split_to_X_y_task>>train_test_split_task>>[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]
[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]>>model_accuracies_task
[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]>>bias_report_task
[bias_report_task,model_accuracies_task]>>model_ranking_task>>select_best_model_task>>test_best_model_task>>register_best_model_task>>prepare_image_folder_task >> build_docker_image_task >> push_image_to_artifact_registry_task >> deploy_to_gke_task >> test_inference_task >>sync_models>>sync_logs >> sync_outputs >> sync_mlrun>>task_send_alert_email
