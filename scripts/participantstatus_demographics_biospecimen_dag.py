from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define file paths
participant_status_path = '/home/mrudula/MLPOPS/data_raw/Participant_Status_27Oct2024.csv'
demographics_path = '/home/mrudula/MLPOPS/data_raw/Demographics_27Oct2024.csv'
biospecimen_analysis_path = '/home/mrudula/MLPOPS/data_raw/SAA_Biospecimen_Analysis_Results_27Oct2024.csv'

# Custom email alert function
def send_custom_alert_email(**context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    try_number = context['task_instance'].try_number
    subject = "Airflow Task Alert - Failure or Retry"
    body = f"Task {task_id} in DAG {dag_id} has failed or retried (Attempt: {try_number})."

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "mrudulaacharya18@gmail.com"
    msg['To'] = "mrudulaacharya18@gmail.com"

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("mrudulaacharya18@gmail.com", "lhwnkkhmvptmjghx")  # Use an app-specific password
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            print("Alert email sent successfully.")
    except Exception as e:
        print(f"Error sending alert email: {e}")

# Define functions for each data processing task
def participant_status_load():
    return pd.read_csv(participant_status_path)

def demographics_load():
    return pd.read_csv(demographics_path)

def clean_participant_status(Participant_Status):
    Participant_Status['ENROLL_DATE'] = pd.to_datetime(Participant_Status['ENROLL_DATE'], format='%m/%Y', errors='coerce')
    participant_status = Participant_Status.rename(columns={"PATNO": "Participant_ID"})
    columns_to_drop = ['COHORT_DEFINITION','STATUS_DATE','INEXPAGE','AV133STDY','TAUSTDY','GAITSTDY','PISTDY','SV2ASTDY','PPMI_ONLINE_ENROLL']
    participant_status.drop(columns=columns_to_drop, inplace=True)
    return participant_status

def clean_demographics(Demographics):
    columns_to_drop = ['REC_ID','EVENT_ID','PAG_NAME','INFODT','AFICBERB','ASHKJEW','BASQUE','BIRTHDT','HOWLIVE', 
                       'GAYLES', 'HETERO','BISEXUAL','PANSEXUAL','ASEXUAL', 'OTHSEXUALITY','ORIG_ENTRY','LAST_UPDATE']
    Demographics.drop(columns=columns_to_drop, inplace=True)
    return Demographics

def merge_participant_status_and_demographics(participant_status, Demographics):
    combined_table = pd.merge(
        participant_status,
        Demographics,
        left_on="Participant_ID",
        right_on="PATNO",
        suffixes=("", "_drop")
    )
    valid_statuses = ['Enrolled', 'Complete', 'Withdrew']
    combined_table = combined_table[combined_table['ENROLL_STATUS'].isin(valid_statuses)]
    return combined_table

def clean_participantstatus_demographic(combined_table):
    columns_to_drop = ['ENROLL_STATUS','PATNO','ENRLLRRK2','ENRLPINK1','HANDED','HISPLAT','RAASIAN','RABLACK','RAHAWOPI',
                       'RAINDALS','RANOS','RAWHITE','RAUNKNOWN']
    combined_table.drop(columns=columns_to_drop, inplace=True)
    return combined_table

def biospecimen_analysis_load():
    return pd.read_csv(biospecimen_analysis_path)

def clean_biospecimen_analysis(Biospecimen_Analysis):
    Biospecimen_Analysis['RUNDATE'] = pd.to_datetime(Biospecimen_Analysis['RUNDATE'], format='%Y-%m-%d', errors='coerce')
    columns_to_drop = ['SEX','COHORT','TYPE','InstrumentRep2','InstrumentRep3','PROJECTID','PI_NAME','PI_INSTITUTION']
    Biospecimen_Analysis.drop(columns=columns_to_drop, inplace=True)
    return Biospecimen_Analysis

def filter_biospecimen_analysis(Biospecimen_Analysis):
    earliest_records = (
        Biospecimen_Analysis[Biospecimen_Analysis['CLINICAL_EVENT'] == 'BL']
        .groupby('PATNO', as_index=False)
        .agg(earliest_date=('RUNDATE', 'min'))
    )
    Biospecimen_Analysis_Cleaned = pd.merge(
        Biospecimen_Analysis,
        earliest_records,
        left_on=['PATNO', 'RUNDATE'],
        right_on=['PATNO', 'earliest_date'],
        how='inner'
    )
    return Biospecimen_Analysis_Cleaned

def clean_filtered_biospecimen_analysis(Biospecimen_Analysis_Cleaned):
    columns_to_drop = ['CLINICAL_EVENT','RUNDATE','earliest_date']
    Biospecimen_Analysis_Cleaned.drop(columns=columns_to_drop, inplace=True)
    return Biospecimen_Analysis_Cleaned

def merge_participantstatus_demographics_biospecimen_analysis(combined_table, Biospecimen_Analysis_Cleaned):
    merged_table = pd.merge(
        combined_table,
        Biospecimen_Analysis_Cleaned,
        left_on='Participant_ID',
        right_on='PATNO',
        how='left'
    )
    return merged_table

def clean_participantstatus_demographics_biospecimen_analysis(merged_table):
    merged_table.drop(columns=['PATNO'], inplace=True)
    return merged_table

# Define the Airflow DAG
motor_merged_task = PythonOperator(
    task_id='motor_merged',
    python_callable=motor_merged,
    provide_context=True,
    dag=dag,
)
with DAG(
    dag_id="data_pipeline_with_custom_email_alerts",
    default_args=default_args,
    description='Data pipeline with custom email alerts for task failures',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_participant_status_load = PythonOperator(
        task_id="participant_status_load",
        python_callable=participant_status_load
    )

    task_demographics_load = PythonOperator(
        task_id="demographics_load",
        python_callable=demographics_load
    )

    task_clean_participant_status = PythonOperator(
        task_id="clean_participant_status",
        python_callable=lambda: clean_participant_status(participant_status_load())
    )

    task_clean_demographics = PythonOperator(
        task_id="clean_demographics",
        python_callable=lambda: clean_demographics(demographics_load())
    )

    task_merge_participant_status_and_demographics = PythonOperator(
        task_id="merge_participant_status_and_demographics",
        python_callable=lambda: merge_participant_status_and_demographics(clean_participant_status(), clean_demographics())
    )

    task_clean_participantstatus_demographic = PythonOperator(
        task_id="clean_participantstatus_demographic",
        python_callable=lambda: clean_participantstatus_demographic(merge_participant_status_and_demographics())
    )

    task_biospecimen_analysis_load = PythonOperator(
        task_id="biospecimen_analysis_load",
        python_callable=biospecimen_analysis_load
    )

    task_clean_biospecimen_analysis = PythonOperator(
        task_id="clean_biospecimen_analysis",
        python_callable=lambda: clean_biospecimen_analysis(biospecimen_analysis_load())
    )

    task_filter_biospecimen_analysis = PythonOperator(
        task_id="filter_biospecimen_analysis",
        python_callable=lambda: filter_biospecimen_analysis(clean_biospecimen_analysis())
    )

    task_clean_filtered_biospecimen_analysis = PythonOperator(
        task_id="clean_filtered_biospecimen_analysis",
        python_callable=lambda: clean_filtered_biospecimen_analysis(filter_biospecimen_analysis())
    )

    task_merge_participantstatus_demographics_biospecimen_analysis = PythonOperator(
        task_id="merge_participantstatus_demographics_biospecimen_analysis",
        python_callable=lambda: merge_participantstatus_demographics_biospecimen_analysis(
            clean_participantstatus_demographic(), clean_filtered_biospecimen_analysis()
        )
    )

    task_clean_participantstatus_demographics_biospecimen_analysis = PythonOperator(
        task_id="clean_participantstatus_demographics_biospecimen_analysis",
        python_callable=lambda: clean_participantstatus_demographics_biospecimen_analysis(
            merge_participantstatus_demographics_biospecimen_analysis()
        )
    )

    send_alert_task = PythonOperator(
        task_id='send_custom_alert_email',
        python_callable=send_custom_alert_email,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    
    task_participant_status_load >> task_clean_participant_status
    task_demographics_load >> task_clean_demographics
    [task_clean_participant_status, task_clean_demographics] >> task_merge_participant_status_and_demographics >> task_clean_participantstatus_demographic
    task_biospecimen_analysis_load >> task_clean_biospecimen_analysis >> task_filter_biospecimen_analysis >> task_clean_filtered_biospecimen_analysis
    [task_clean_filtered_biospecimen_analysis, task_clean_participantstatus_demographic] >> task_merge_participantstatus_demographics_biospecimen_analysis >> task_clean_participantstatus_demographics_biospecimen_analysis
