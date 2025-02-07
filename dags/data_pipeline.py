import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import logging
import joblib
logging.basicConfig(level=logging.INFO)
# Default arguments for the DAG.
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
    'data_pipeline',
    default_args=default_args,
    description='Data pipeline with custom email alerts for task failures',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define file paths
participant_status_path = '/opt/airflow/raw_data/Participant_Status_05Dec2024.csv'
demographics_path = '/opt/airflow/raw_data/Demographics_05Dec2024.csv'
biospecimen_analysis_path = '/opt/airflow/raw_data/SAA_Biospecimen_Analysis_Results_05Dec2024.csv'
# Directory where CSV files are stored.
csv_directory = '/opt/airflow/motor_assessment/'



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



# Define functions for each data processing task
def participant_status_load(**context):
    try:

        logging.info("Loading participant status data...")
        participant_status = pd.read_csv(participant_status_path)
        context['ti'].xcom_push(key='participant_status', value=participant_status)
        logging.info("Participant status data loaded successfully.")        
        return participant_status
    except Exception as e:
        logging.error("Error loading participant status data: %s", e)
        raise

def demographics_load(**context):

    try:
        logging.info("Loading demographics data...")
        demographics = pd.read_csv(demographics_path)
        context['ti'].xcom_push(key='demographics', value=demographics)
        logging.info("Demographics data loaded successfully.")
        return demographics
    except Exception as e:
        logging.error("Error loading demographics data: %s", e)
        raise

def clean_participant_status(**context):
    try:
        logging.info("Cleaning participant status data...")
        participant_status = context['ti'].xcom_pull(task_ids='task_participant_status_load', key='participant_status')
        participant_status['ENROLL_DATE'] = pd.to_datetime(participant_status['ENROLL_DATE'], format='%m/%Y', errors='coerce')
        participant_status = participant_status.rename(columns={"PATNO": "Participant_ID"})
        columns_to_drop = ['COHORT_DEFINITION','STATUS_DATE','INEXPAGE','AV133STDY','TAUSTDY','GAITSTDY','PISTDY','SV2ASTDY','PPMI_ONLINE_ENROLL']
        participant_status.drop(columns=columns_to_drop, inplace=True)
        context['ti'].xcom_push(key='cleaned_participant_status', value=participant_status)
        logging.info("Participant status data cleaned successfully.")
        return participant_status
    except Exception as e:
        logging.error("Error cleaning participant status data: %s", e)
        raise

def clean_demographics(**context):
     try:
        logging.info("Cleaning demographics data...")
        demographics = context['ti'].xcom_pull(task_ids='task_demographics_load', key='demographics')
        columns_to_drop = ['REC_ID','EVENT_ID','PAG_NAME','INFODT','AFICBERB','ASHKJEW','BASQUE','BIRTHDT','HOWLIVE', 
                        'GAYLES', 'HETERO','BISEXUAL','PANSEXUAL','ASEXUAL', 'OTHSEXUALITY','ORIG_ENTRY','LAST_UPDATE']
        demographics.drop(columns=columns_to_drop, inplace=True)
        context['ti'].xcom_push(key='cleaned_demographics', value=demographics)
        logging.info("Demographics data cleaned successfully.")
        return demographics
     except Exception as e:
        logging.error("Error cleaning demographics data: %s", e)
        raise


def merge_participant_status_and_demographics(**context):
    try:
        logging.info("Merging participant status and demographics data...") 
        participant_status = context['ti'].xcom_pull(task_ids='task_clean_participant_status', key='cleaned_participant_status')
        demographics = context['ti'].xcom_pull(task_ids='task_clean_demographics', key='cleaned_demographics')
        combined_table = pd.merge(
            participant_status,
            demographics,
            left_on="Participant_ID",
            right_on="PATNO",
            suffixes=("", "_drop")
        )
        valid_statuses = ['Enrolled', 'Complete', 'Withdrew']
        combined_table = combined_table[combined_table['ENROLL_STATUS'].isin(valid_statuses)]
        context['ti'].xcom_push(key='combined_table', value=combined_table)
        logging.info("Participant status and demographics data merged successfully.")
        return combined_table
    
    except Exception as e:
        logging.error("Error merging participant status and demographics data: %s", e)
        raise


def clean_participantstatus_demographic(**context):
    try:
        logging.info("Cleaning merged participant status and demographics data...")
       
        combined_table = context['ti'].xcom_pull(task_ids='task_merge_participant_status_and_demographics', key='combined_table')
        columns_to_drop = ['ENROLL_STATUS','PATNO','ENRLLRRK2','ENRLPINK1','HANDED','HISPLAT','RAASIAN','RABLACK','RAHAWOPI',
                        'RAINDALS','RANOS','RAWHITE','RAUNKNOWN']
        combined_table.drop(columns=columns_to_drop, inplace=True)
        context['ti'].xcom_push(key='cleaned_participantstatus_demographic', value=combined_table)
        logging.info("Merged participant status and demographics data cleaned successfully.")
   
        return combined_table
    except Exception as e:
        logging.error("Error cleaning merged participant status and demographics data: %s", e)
        raise

def biospecimen_analysis_load(**context):
    try:
        logging.info("Loading biospecimen analysis data...")
        biospecimen_analysis = pd.read_csv(biospecimen_analysis_path)
        context['ti'].xcom_push(key='biospecimen_analysis', value=biospecimen_analysis)
        logging.info("Biospecimen analysis data loaded successfully.")
        return biospecimen_analysis
    except Exception as e:
        logging.error("Error loading biospecimen analysis data: %s", e)
        raise

def clean_biospecimen_analysis(**context):
    try:
        logging.info("Cleaning biospecimen analysis data...")
        biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_biospecimen_analysis_load', key='biospecimen_analysis')
        biospecimen_analysis['RUNDATE'] = pd.to_datetime(biospecimen_analysis['RUNDATE'], format='%Y-%m-%d', errors='coerce')
        columns_to_drop = ['SEX','COHORT','TYPE','InstrumentRep2','InstrumentRep3','PROJECTID','PI_NAME','PI_INSTITUTION']
        biospecimen_analysis.drop(columns=columns_to_drop, inplace=True)
        context['ti'].xcom_push(key='cleaned_biospecimen_analysis', value=biospecimen_analysis)
        logging.info("Biospecimen analysis data cleaned successfully.")
        return biospecimen_analysis
    except Exception as e:
        logging.error("Error cleaning biospecimen analysis data: %s", e)
        raise

def filter_biospecimen_analysis(**context):
    try:
        logging.info("Filtering biospecimen analysis data...")
        biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_clean_biospecimen_analysis', key='cleaned_biospecimen_analysis')
        
        bl_events = biospecimen_analysis[biospecimen_analysis['CLINICAL_EVENT'] == 'BL']

        # Step 2: Convert 'RUNDATE' to datetime if it's not already in datetime format
        bl_events['RUNDATE'] = pd.to_datetime(bl_events['RUNDATE'])  # Adjust format if necessary

        # Step 3: Sort the filtered DataFrame by PATNO and RUNDATE to ensure chronological order
        bl_events_sorted = bl_events.sort_values(by=['PATNO', 'RUNDATE'])

        # Step 4: Drop duplicates based on 'PATNO', keeping the first (earliest) occurrence
        biospecimen_analysis_cleaned = bl_events_sorted.drop_duplicates(subset='PATNO', keep='first')

        context['ti'].xcom_push(key='filtered_biospecimen_analysis', value=biospecimen_analysis_cleaned)
        logging.info("Biospecimen analysis data filtered successfully.")

        return biospecimen_analysis_cleaned
    
    except Exception as e:
        logging.error("Error filtering biospecimen analysis data: %s", e)
        raise

def clean_filtered_biospecimen_analysis(**context):
    try:
        logging.info("Cleaning biospecimen analysis data ..")
        biospecimen_analysis_cleaned = context['ti'].xcom_pull(task_ids='task_filter_biospecimen_analysis', key='filtered_biospecimen_analysis')
        columns_to_drop = ['CLINICAL_EVENT','RUNDATE']
        biospecimen_analysis_cleaned.drop(columns=columns_to_drop, inplace=True)
        context['ti'].xcom_push(key='cleaned_filtered_biospecimen_analysis', value=biospecimen_analysis_cleaned)
        logging.info("Cleaned merged participant and biospecimen data")
        return biospecimen_analysis_cleaned
    
    except Exception as e:
        logging.error(f"Error in clean_filtered_biospecimen_analysis: {str(e)}")
        raise


def merge_biospecimen_with_participant(**context):
    try:
        logging.info("Starting merge_biospecimen_with_participant task")
        
        biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_clean_filtered_biospecimen_analysis', key='cleaned_filtered_biospecimen_analysis')
        combined_table = context['ti'].xcom_pull(task_ids='task_clean_participantstatus_demographic', key='cleaned_participantstatus_demographic')
        if biospecimen_analysis is None:
            raise ValueError("biospecimen_analysis is None. Check the task 'task_clean_filtered_biospecimen_analysis'.")
        if combined_table is None:
            raise ValueError("combined_table is None. Check the task 'task_clean_participantstatus_demographic'.")
        
        merged_data = pd.merge(
            combined_table,
            biospecimen_analysis,
            left_on='Participant_ID',
            right_on='PATNO',
            how='left'
        )
        context['ti'].xcom_push(key='merged_data', value=merged_data)
        logging.info("Successfully merged biospecimen data with participant data")
    
        return merged_data
    except Exception as e:
        logging.error(f"Error in merge_biospecimen_with_participant: {str(e)}")
        raise

def clean_participantstatus_demographics_biospecimen_analysis(**context):
    try:
        logging.info("Starting clean_participantstatus_demographics_biospecimen_analysis task")
    
        merged_data = context['ti'].xcom_pull(task_ids='task_merge_participantstatus_demographics_biospecimen_analysis', key='merged_data')
        merged_data.drop(columns=['PATNO'], inplace=True)
        context['ti'].xcom_push(key='merged_data_cleaned', value=merged_data)
        #merged_data.to_csv('/home/mrudula/MLPOPS/outputs/patient_demographics_biospecimen.csv', index=False)
        logging.info("Cleaned merged participant and biospecimen data")      
        return merged_data

    except Exception as e:
            logging.error(f"Error in clean_participantstatus_demographics_biospecimen_analysis: {str(e)}")
            raise


# Load functions for each CSV file
def load_motor_senses_1(**context):
    try:
        logging.info("Loading motor senses data from MDS-UPDRS_Part_I")

        return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_05Dec2024.csv'))
    except Exception as e:
        logging.error(f"Error loading motor senses data for Part 1: {str(e)}")
        raise

def load_motor_senses_2(**context):
    try:
        logging.info("Loading motor senses data from MDS-UPDRS_Part_I_Patient_Questionnaire")

        return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_Patient_Questionnaire_05Dec2024.csv'))
    except Exception as e:
        logging.error(f"Error loading motor senses data for Part 2: {str(e)}")
        raise
def load_motor_senses_3(**context):
    try:
        logging.info("Loading motor senses data from MDS_UPDRS_Part_II__Patient_Questionnaire")

        return pd.read_csv(os.path.join(csv_directory, 'MDS_UPDRS_Part_II__Patient_Questionnaire_05Dec2024.csv'))
    except Exception as e:
        logging.error(f"Error loading motor senses data for Part 3: {str(e)}")
        raise

def load_motor_senses_4(**context):
    try:
        logging.info("Loading motor senses data from MDS-UPDRS_Part_III")

        return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_III_05Dec2024.csv'))
    except Exception as e:
        logging.error(f"Error loading motor senses data for Part 4: {str(e)}")
        raise

def load_motor_senses_5(**context):
    try:
        logging.info("Loading motor senses data from MDS-UPDRS_Part_IV__Motor_Complications")

        return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_IV__Motor_Complications_05Dec2024.csv'))
    except Exception as e:
        logging.error(f"Error loading motor senses data for Part 5: {str(e)}")
        raise
# Clean functions for each loaded CSV
def clean_motor_senses_1(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    try:

        logging.info("Starting clean_motor_senses_1 task")
        df = context['ti'].xcom_pull(task_ids='load_motor_senses_1_task')

        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
    
    
        df.drop(columns=['REC_ID','PAG_NAME','INFODT','LAST_UPDATE','NP1RTOT'],inplace=True)

        logging.info("Cleaned motor senses Part 1 data and pushed to XCom")
    
        context['ti'].xcom_push(key='cleaned_1', value=df)

        # Drop specified columns and return the cleaned DataFrame
        return df
    
    except Exception as e:
            logging.error(f"Error in clean_motor_senses_1: {str(e)}")
            raise
def clean_motor_senses_2(**context):
    try:

        logging.info("Starting clean_motor_senses_2 task")
    # Pull the DataFrame from XCom, making sure it's a DataFrame
        df = context['ti'].xcom_pull(task_ids='load_motor_senses_2_task')

        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        df.drop(columns=['REC_ID','PAG_NAME','INFODT','LAST_UPDATE','NP1PTOT'],inplace=True)
        logging.info("Cleaned motor senses Part 2 data and pushed to XCom")
        
        #df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP1PTOT'],inplace=True)
        context['ti'].xcom_push(key='cleaned_2', value=df)
        # Drop specified columns and return the cleaned DataFrame
        return df
    except Exception as e:
            logging.error(f"Error in clean_motor_senses_2: {str(e)}")
            raise

def clean_motor_senses_3(**context):

    try:

        logging.info("Starting clean_motor_senses_3 task")
        # Pull the DataFrame from XCom, making sure it's a DataFrame
        df = context['ti'].xcom_pull(task_ids='load_motor_senses_3_task')

        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)

        # Drop specified columns and return the cleaned DataFrame

        df.drop(columns=['REC_ID','PAG_NAME','INFODT','LAST_UPDATE','NP2PTOT'],inplace=True)
        #df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP2PTOT'],inplace=True)
        logging.info("Cleaned motor senses Part 3 data and pushed to XCom")
        
        context['ti'].xcom_push(key='cleaned_3', value=df)
        return df

    except Exception as e:
            logging.error(f"Error in clean_motor_senses_3: {str(e)}")
            raise
def clean_motor_senses_4(**context):
    try:

        logging.info("Starting clean_motor_senses_4 task")
    # Pull the DataFrame from XCom, making sure it's a DataFrame
        df = context['ti'].xcom_pull(task_ids='load_motor_senses_4_task')

        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        df.drop(columns=['REC_ID','PAG_NAME','INFODT','LAST_UPDATE','PDTRTMNT','PDSTATE','HRPOSTMED','HRDBSON','HRDBSOFF','PDMEDYN','DBSYN','ONOFFORDER','OFFEXAM','OFFNORSN','DBSOFFTM','ONEXAM','ONNORSN','DBSONTM','PDMEDDT','PDMEDTM','EXAMDT','EXAMTM','NP3TOT'],inplace=True)
        #df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','PDTRTMNT','PDSTATE','HRPOSTMED','HRDBSON','HRDBSOFF','PDMEDYN','DBSYN','ONOFFORDER','OFFEXAM','OFFNORSN','DBSOFFTM','ONEXAM','ONNORSN','DBSONTM','PDMEDDT','PDMEDTM','EXAMDT','EXAMTM','NP3TOT'],inplace=True)

        logging.info("Cleaned motor senses Part 4 data and pushed to XCom")
        
        context['ti'].xcom_push(key='cleaned_4', value=df)
        # Drop specified columns and return the cleaned DataFrame
        return df
    except Exception as e:
            logging.error(f"Error in clean_motor_senses_4: {str(e)}")
            raise

def clean_motor_senses_5(**context):
    try:

        logging.info("Starting clean_motor_senses_5 task")
    # Pull the DataFrame from XCom, making sure it's a DataFrame
        df = context['ti'].xcom_pull(task_ids='load_motor_senses_5_task')

        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        #df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP4TOT'],inplace=True)
        df.drop(columns=['REC_ID','PAG_NAME','INFODT','LAST_UPDATE','NP4TOT'],inplace=True)
        logging.info("Cleaned motor senses Part 5 data and pushed to XCom")
        
        context['ti'].xcom_push(key='cleaned_5', value=df)
        # Drop specified columns and return the cleaned DataFrame
        return df
    except Exception as e:
            logging.error(f"Error in clean_motor_senses_5: {str(e)}")
            raise
# Function to merge all cleaned CSVs
def filter_all_motor_senses_csvs(**context):
    # Pull cleaned DataFrames from XCom
    try:

        logging.info("Starting filter_all_motor_senses_csvs task")

        cleaned_dfs = [
            context['ti'].xcom_pull(key='cleaned_1',task_ids='clean_motor_senses_1_task'),
            context['ti'].xcom_pull(key='cleaned_2',task_ids='clean_motor_senses_2_task'),
            context['ti'].xcom_pull(key='cleaned_3',task_ids='clean_motor_senses_3_task'),
            context['ti'].xcom_pull(key='cleaned_4',task_ids='clean_motor_senses_4_task'),
            context['ti'].xcom_pull(key='cleaned_5',task_ids='clean_motor_senses_5_task')
        ]
        #filtered_dfs = [df[df['EVENT_ID'].isin(['BL', 'PW', 'SC', 'ST'])] for df in cleaned_dfs]
        
        # Define a function to clean each DataFrame
        def clean_df(df):
            # Step 1: Convert ORIG_ENTRY to datetime format
            df['ORIG_ENTRY'] = pd.to_datetime(df['ORIG_ENTRY'], format='%m/%Y')
            
            # Step 3: Sort by PATNO and ORIG_ENTRY
            df = df.sort_values(by=['PATNO', 'ORIG_ENTRY'])
            
            # Step 4: Drop duplicates based on PATNO, keeping the first occurrence (earliest date)
            df = df.drop_duplicates(subset=['PATNO'], keep='first')

            df.drop(columns=['ORIG_ENTRY'], inplace=True)
            
            return df
        
        # Apply cleaning function to each DataFrame
        filtered_dfs = [clean_df(df) for df in cleaned_dfs]
        context['ti'].xcom_push(key='filtered_dfs', value=filtered_dfs)
        logging.info("Filtered and cleaned motor senses data")
        print("Merged DataFrame pushed to XCom")
    except Exception as e:
        logging.error(f"Error in filter_all_motor_senses_csvs: {str(e)}")
        raise


def merge_all_motor_senses_csvs(**context):
    try:

        logging.info("Starting merge_all_motor_senses_csvs task")

        cleaned_dfs = context['ti'].xcom_pull(key='filtered_dfs', task_ids='filter_all_motor_senses_csvs_task')
        
        if not cleaned_dfs:
            raise ValueError("No cleaned DataFrames retrieved from XCom for filtering.")
        
        # Start with the first DataFrame
        merged_df = cleaned_dfs[0]    
        
        # Perform the merge with outer join
        for df in cleaned_dfs[1:]:
            merged_df = merged_df.merge(df, on='PATNO', how='outer', suffixes=('', '_dup'))
        # Dropping duplicate columns created during merging
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        
        # Push the merged DataFrame to XCom
        context['ti'].xcom_push(key='filter_merged_df', value=merged_df)
        print("Filtering done and merged DataFrame pushed to XCom!")
        logging.info("Successfully merged motor senses data")
    except Exception as e:
        logging.error(f"Error in merge_all_motor_senses_csvs: {str(e)}")
        raise


    

def drop_duplicate_motor_senses_columns(**context):

    try:
        logging.info("Starting drop_duplicate_motor_senses_columns task")
    
        # Retrieve merged DataFrame from XCom
        merged_df = context['ti'].xcom_pull(key='filter_merged_df', task_ids='merge_all_motor_senses_csvs_task')
        if merged_df is None:
            raise ValueError("No cleaned DataFrames retrieved from XCom for filtering.")
        
        # Drop duplicate columns
        deduped_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        deduped_df=merged_df.drop(columns=['EVENT_ID','EVENT_ID_dup','NUPSOURC_dup'])
        
        # Save the deduplicated DataFrame
        deduped_path = os.path.join(csv_directory, 'merged_deduped_file.csv')
        #deduped_df.to_csv('/home/mrudula/MLPOPS/outputs/motor_senses_merged', index=False)
        context['ti'].xcom_push(key='deduped_df', value=deduped_df)
        
        print(f"Deduplicated merged file saved at {deduped_path}")
        logging.info(f"Deduplicated merged file saved at {deduped_path}")

        return deduped_df
    except Exception as e:
        logging.error(f"Error in drop_duplicate_motor_senses_columns: {str(e)}")
        raise


def load_and_merge_data(**context):

    try:
        logging.info("Load and merge participant and motor sense")
        participantstatus_demographics_biospecimen_merged_cleaned = context['ti'].xcom_pull(task_ids='task_clean_participantstatus_demographics_biospecimen_analysis', key='merged_data_cleaned')
        motor_senses_merged_cleaned= context['ti'].xcom_pull(task_ids='deduplication_motor_senses_task',key='deduped_df')
        # Perform the merging operation
        merged_df_final = pd.merge(
            participantstatus_demographics_biospecimen_merged_cleaned,
            motor_senses_merged_cleaned,
            left_on="Participant_ID",
            right_on="PATNO",
            how='left'
        )
        merged_df_final.drop(columns=['PATNO'],inplace=True)
        context['ti'].xcom_push(key='merged_final', value=merged_df_final)
        #merged_df_final.to_csv('/home/mrudula/MLPOPS/outputs/final_cleaned.csv', index=False)
        logging.info("Megre succesfull")
        return merged_df_final
    except Exception as e:
        logging.error(f"Error in load_and_merge_data: {str(e)}")
        raise

def seperate_target_values(**context):

    try:
        logging.info("Seperate target variables from data")
        df=context['ti'].xcom_pull(task_ids='load_and_merge_data',key='merged_final')
        
        
        target_col=df['COHORT']
        df.drop(columns=['COHORT'],inplace=True)
        context['ti'].xcom_push(key='data', value=df)
        context['ti'].xcom_push(key='target_col', value= target_col.tolist())
        logging.info("Successfull")
        return target_col.tolist(),df
    except Exception as e:
        logging.error(f"seperate_target_values: {str(e)}")
        raise


# Task 3: Data cleaning, preprocessing, and EDA
def missing_values_drop(**context):
    try:
        logging.info("Drop missing values")
        data_drop= context['ti'].xcom_pull(task_ids='seperate_target_values',key='data') 
        if isinstance(data_drop, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            data_drop = pd.read_json(data_drop)
        
        threshold = 0.5
        cols_to_drop = [col for col in data_drop.columns if data_drop[col].isnull().mean() > threshold]
        data_drop.drop(columns=cols_to_drop,inplace=True)
        print(f"Dropped columns with > 50% missing values: {cols_to_drop}")
        context['ti'].xcom_push(key='data_drop', value= data_drop)  
        logging.info("Successfully finished dropping")  
        return data_drop
    except Exception as e:
        logging.error(f"missing_values_drop: {str(e)}")
        raise

def seperate_categorical_columns(**context):
    try:
        logging.info("Seperate categorcial columns")
        df= context['ti'].xcom_pull(task_ids='missing_values_drop',key='data_drop') 
        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        
        threshold = 9  # Define a threshold based on knowledge of the data
        categorical_cols = [col for col in df.columns if df[col].nunique() < threshold]
        print(categorical_cols)
        context['ti'].xcom_push(key='categorical_cols', value= categorical_cols)    
        logging.info("Done seperating categorical columns")
        return categorical_cols
    except Exception as e:
        logging.error(f"seperate_categorical_columns: {str(e)}")
        raise
    

def seprerate_numerical_columns(**context):
    try:
        logging.info("Seperate numerical columns")

        df= context['ti'].xcom_pull(task_ids='missing_values_drop',key='data_drop') 
        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        
        categorical_list= context['ti'].xcom_pull(task_ids='seperate_categorical_columns',key='categorical_cols') 
        if isinstance(categorical_list, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            categorical_list = pd.read_json(categorical_list)
        
        numerical_cols = [col for col in df.columns if (col not in categorical_list and df[col].dtype in ['int64', 'float64'])]
        context['ti'].xcom_push(key='numerical_cols', value= numerical_cols)  
        logging.info("Done seperating numerical columns")
        return numerical_cols
    except Exception as e:
            logging.error(f"seperate_categorical_columns: {str(e)}")
            raise

def missing_values_impute_5percent(**context):
    try:
        logging.info("Imputing missing values 5%")
        
        df= context['ti'].xcom_pull(task_ids='missing_values_drop',key='data_drop') 
        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        
        categorical_cols= context['ti'].xcom_pull(task_ids='seperate_categorical_columns',key='categorical_cols') 
        if isinstance(categorical_cols, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            categorical_cols = pd.read_json(categorical_cols)
        
        numerical_cols=context['ti'].xcom_pull(task_ids='seprerate_numerical_columns',key='numerical_cols') 
        if numerical_cols is None:
            raise ValueError("numerical_cols is None, make sure 'seperate_numerical_columns' task pushed a valid list.")
        
        numerical_cols_minimal_missing=[]
        if numerical_cols:
            numerical_cols_minimal_missing = [col for col in numerical_cols if df[col].isnull().mean() < 0.05]
        categorical_cols_minimal_missing = [col for col in categorical_cols if df[col].isnull().mean() < 0.05]

        # Impute numerical columns with the median
        for col in numerical_cols_minimal_missing:
            median_value = df[col].median()  # Calculate median
            df[col] = df[col].fillna(median_value)  # Impute missing values with the median

        # Impute categorical columns with the mode
        for col in categorical_cols_minimal_missing:
            mode_value = df[col].mode()[0]  # Calculate mode (mode() returns a series, so take the first value)
            df[col] = df[col].fillna(mode_value)  # Impute missing values with the mode
        context['ti'].xcom_push(key='df', value= df)
        logging.info("Imputed missing values 5%")
        return df
    except Exception as e:
                logging.error(f" Missing_values_impute_5percent {str(e)}")
                raise


def drop_correlated_unrelated_columns(**context):

    try:
        logging.info("Drop unrealted columns")

        df= context['ti'].xcom_pull(task_ids='missing_values_impute_5percent',key='df') 
        numerical_cols=context['ti'].xcom_pull(task_ids='seprerate_numerical_columns',key='numerical_cols') 
        if numerical_cols is None:
            raise ValueError("numerical_cols is None, make sure 'seperate_numerical_columns' task pushed a valid list.")
        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        df.drop(columns=['Participant_ID', 'ENROLL_DATE'],inplace=True)
        numerical_cols.remove('Participant_ID')
        
        context['ti'].xcom_push(key='numerical_cols', value= numerical_cols) 
        context['ti'].xcom_push(key='drop_correlated_unrelated_columns', value= df)
        logging.info("Sucessfully dropped unrelated columns")
        return df,numerical_cols
    except Exception as e:
                logging.error(f"Drop_correlated_unrelated_columns  {str(e)}")
                raise


def missing_values_impute_50percent_scaling(**context):      
        
    try:
        logging.info("Imputing missing values 50%")
    
        df= context['ti'].xcom_pull(task_ids='drop_correlated_unrelated_columns',key='drop_correlated_unrelated_columns') 
        if isinstance(df, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            df = pd.read_json(df)
        categorical_cols= context['ti'].xcom_pull(task_ids='seperate_categorical_columns',key='categorical_cols') 
        if isinstance(categorical_cols, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            categorical_cols = pd.read_json(categorical_cols)
        
        numerical_cols= context['ti'].xcom_pull(task_ids='drop_correlated_unrelated_columns',key='numerical_cols')
        if isinstance(numerical_cols, str):
            # If df is a string, it’s likely being serialized; read it as DataFrame
            numerical_cols = pd.read_json(numerical_cols)
        
        one_hot_encoder = OneHotEncoder(handle_unknown='ignore')  # Drop first to avoid collinearity
        scaler = StandardScaler()
        numerical_pipeline = Pipeline(steps=[
        ('scaler', scaler)
        ])

        # Categorical pipeline: Impute using KNN, then apply one-hot encoding
        categorical_pipeline = Pipeline(steps=[
            ('knn_impute', KNNImputer(n_neighbors=5)),  # Apply KNN imputation to categorical columns with 5-50% missing values
            ('one_hot', one_hot_encoder)  # One-hot encode categorical columns
        ])

        # Use ColumnTransformer to apply the different pipelines to numerical and categorical columns
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numerical_pipeline, numerical_cols),  # Apply numerical pipeline to numerical columns
                ('cat', categorical_pipeline, categorical_cols)  # Apply categorical pipeline to categorical columns
            ],
            remainder='passthrough'  # Keep other columns like date intact (without changes)
        )
        df_preprocessed = preprocessor.fit_transform(df)

        preprocessor_filename = f"/opt/airflow/models/preprocessor.pkl"
        joblib.dump(preprocessor, preprocessor_filename)
        # Retrieve column names after one-hot encoding and scaling
        one_hot_feature_names = preprocessor.transformers_[1][1].named_steps['one_hot'].get_feature_names_out(categorical_cols)
        # Combine column names
        all_feature_names = numerical_cols + one_hot_feature_names.tolist() + [col for col in df.columns if col not in numerical_cols + categorical_cols]
        # Convert the ndarray to pandas DataFrame with proper column names
        df_preprocessed_df = pd.DataFrame(df_preprocessed.toarray(), columns=all_feature_names)

        context['ti'].xcom_push(key='missing_values_impute_50percent_scaling_df', value= df_preprocessed_df)
        logging.info("Imputed missing values 50%")
        return df_preprocessed_df
    
    except Exception as e:
                logging.error(f"Missing_values_impute_50percent   {str(e)}")
                raise

def concatenate_df_target(**context):

    try:
        logging.info("Final dataset")

        df_final=context['ti'].xcom_pull(key='missing_values_impute_50percent_scaling_df',task_ids='missing_values_impute_50percent_scaling')
        
        target_col= context['ti'].xcom_pull(key='target_col', task_ids='seperate_target_values')
        df_final['COHORT']= target_col
        df_final.to_csv('/opt/airflow/outputs/airflow_cleaned_data1.csv', index=False)
        df_final=df_final.to_json(orient='split')
        context['ti'].xcom_push(key='df_final', value= df_final)
        logging.info("Final dataset created")
        return df_final
    except Exception as e:
                logging.error(f"Couldn't create final dataset   {str(e)}")
                raise


# Task 0: Load participant status
task_participant_status_load = PythonOperator(
    task_id='task_participant_status_load',
    python_callable=participant_status_load,
    provide_context=True,
    dag=dag,
)

# Task 1: Load demographics
task_demographics_load = PythonOperator(
    task_id='task_demographics_load',
    python_callable=demographics_load,
    provide_context=True,
    dag=dag,
)

# Task 2: Clean participant status
task_clean_participant_status = PythonOperator(
    task_id='task_clean_participant_status',
    python_callable=clean_participant_status,
    provide_context=True,
    dag=dag,
)

# Task 3: Clean demographics
task_clean_demographics = PythonOperator(
    task_id='task_clean_demographics',
    python_callable=clean_demographics,
    provide_context=True,
    dag=dag,
)

# Task 4: Merge participant status and demographics
task_merge_participant_status_and_demographics = PythonOperator(
    task_id='task_merge_participant_status_and_demographics',
    python_callable=merge_participant_status_and_demographics,
    provide_context=True,
    dag=dag,
)

# Task 5: Clean participant status and demographics merged table
task_clean_participantstatus_demographic = PythonOperator(
    task_id='task_clean_participantstatus_demographic',
    python_callable=clean_participantstatus_demographic,
    provide_context=True,
    dag=dag,
    
)

# Task 6: Load Biospecimen Analysis Data
task_biospecimen_analysis_load = PythonOperator(
    task_id='task_biospecimen_analysis_load',
    python_callable=biospecimen_analysis_load,
    provide_context=True,
    dag=dag
)

# Task 7: Clean Biospecimen Analysis Data
task_clean_biospecimen_analysis = PythonOperator(
    task_id='task_clean_biospecimen_analysis',
    python_callable=clean_biospecimen_analysis,
    provide_context=True,
    dag=dag
)

# Task 8: Filter Biospecimen Analysis Data
task_filter_biospecimen_analysis = PythonOperator(
    task_id='task_filter_biospecimen_analysis',
    python_callable=filter_biospecimen_analysis,
    provide_context=True,
    dag=dag
)

# Task 9: Clean Filtered Biospecimen Analysis Data
task_clean_filtered_biospecimen_analysis = PythonOperator(
    task_id='task_clean_filtered_biospecimen_analysis',
    python_callable=clean_filtered_biospecimen_analysis,
    provide_context=True,
    dag=dag
)


# Task 10: Merge biospecimen analysis with participant status and demographics
task_merge_participantstatus_demographics_biospecimen_analysis = PythonOperator(
    task_id='task_merge_participantstatus_demographics_biospecimen_analysis',
    python_callable=merge_biospecimen_with_participant,
    provide_context=True,
    dag=dag,
)

# Task 11: Final clean-up task
task_clean_participantstatus_demographics_biospecimen_analysis = PythonOperator(
    task_id='task_clean_participantstatus_demographics_biospecimen_analysis',
    python_callable=clean_participantstatus_demographics_biospecimen_analysis,
    provide_context=True,
    dag=dag,
)

# Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)
# Load tasks
load_motor_senses_1_task = PythonOperator(
    task_id='load_motor_senses_1_task',
    python_callable=load_motor_senses_1,
    provide_context=True,
    dag=dag,
)

load_motor_senses_2_task = PythonOperator(
    task_id='load_motor_senses_2_task',
    python_callable=load_motor_senses_2,
    provide_context=True,
    dag=dag,
)

load_motor_senses_3_task = PythonOperator(
    task_id='load_motor_senses_3_task',
    python_callable=load_motor_senses_3,
    provide_context=True,
    dag=dag,
)

load_motor_senses_4_task = PythonOperator(
    task_id='load_motor_senses_4_task',
    python_callable=load_motor_senses_4,
    provide_context=True,
    dag=dag,
)

load_motor_senses_5_task = PythonOperator(
    task_id='load_motor_senses_5_task',
    python_callable=load_motor_senses_5,
    provide_context=True,
    dag=dag,
)

# Clean tasks
clean_motor_senses_1_task = PythonOperator(
    task_id='clean_motor_senses_1_task',
    python_callable=clean_motor_senses_1,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_2_task = PythonOperator(
    task_id='clean_motor_senses_2_task',
    python_callable=clean_motor_senses_2,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_3_task = PythonOperator(
    task_id='clean_motor_senses_3_task',
    python_callable=clean_motor_senses_3,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_4_task = PythonOperator(
    task_id='clean_motor_senses_4_task',
    python_callable=clean_motor_senses_4,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_5_task = PythonOperator(
    task_id='clean_motor_senses_5_task',
    python_callable=clean_motor_senses_5,
    provide_context=True,
    dag=dag,
)
#Filter task
filter_all_motor_senses_csvs_task = PythonOperator(
    task_id='filter_all_motor_senses_csvs_task',
    python_callable=filter_all_motor_senses_csvs,
    provide_context=True,
    dag=dag,
)
# Merge task
merge_all_motor_senses_csvs_task = PythonOperator(
    task_id='merge_all_motor_senses_csvs_task',
    python_callable=merge_all_motor_senses_csvs,
    provide_context=True,
    dag=dag,
)

# Deduplication task that pulls merged DataFrame from XCom
deduplication_motor_senses_task = PythonOperator(
    task_id='deduplication_motor_senses_task',
    python_callable=drop_duplicate_motor_senses_columns,
    provide_context=True,
    dag=dag,
)
load_and_merge_task = PythonOperator(
    task_id='load_and_merge_data',
    python_callable=load_and_merge_data,
    provide_context=True,
    dag=dag,
)

seperate_target_values_task = PythonOperator(
    task_id='seperate_target_values',
    python_callable=seperate_target_values,
    provide_context=True,
    dag=dag,
)
missing_values_drop_task = PythonOperator(
    task_id='missing_values_drop',
    python_callable=missing_values_drop,
    provide_context=True,
    dag=dag,
)
seperate_categorical_columns_task = PythonOperator(
    task_id='seperate_categorical_columns',
    python_callable=seperate_categorical_columns,
    provide_context=True,
    dag=dag,
)
seprerate_numerical_columns_task = PythonOperator(
    task_id='seprerate_numerical_columns',
    python_callable=seprerate_numerical_columns,
    provide_context=True,
    dag=dag,
)
missing_values_impute_5percent_task = PythonOperator(
    task_id='missing_values_impute_5percent',
    python_callable=missing_values_impute_5percent,
    provide_context=True,
    dag=dag,
)
drop_correlated_unrelated_columns_task=PythonOperator(
    task_id='drop_correlated_unrelated_columns',
    python_callable = drop_correlated_unrelated_columns,
    provide_context=True,
    dag=dag,

)
missing_values_impute_50percent_scaling_task=PythonOperator(
    task_id='missing_values_impute_50percent_scaling',
    python_callable = missing_values_impute_50percent_scaling,
    provide_context=True,
    dag=dag,

)

concatenate_df_target_task=PythonOperator(
    task_id='concatenate_df_target',
    python_callable = concatenate_df_target,
    provide_context=True,
    dag=dag,

)



 # Task to log data with DVC
# dvc_log_task = BashOperator(
#         task_id='log_data_with_dvc',
#         bash_command="""
#         dvc add /opt/airflow/raw_data/Demographics_27Oct2024.csv && \
#         dvc add /opt/airflow/raw_data/Participant_Status_27Oct2024.csv && \
#         dvc add /opt/airflow/raw_data/SAA_Biospecimen_Analysis_Results_27Oct2024.csv && \
#         dvc add /opt/airflow/motor_assessments/MDS_UPDRS_Part_II__Patient_Questionnaire_27Oct2024.csv && \
#         dvc add /opt/airflow/motor_assessments/MDS-UPDRS_Part_I_27Oct2024.csv && \
#         dvc add /opt/airflow/motor_assessments/MDS-UPDRS_Part_I_Patient_Questionnaire_27Oct2024.csv && \
#         dvc add /opt/airflow/motor_assessments/MDS-UPDRS_Part_III_27Oct2024.csv && \
#         dvc add /opt/airflow/motor_assessments/MDS-UPDRS_Part_IV__Motor_Complications_27Oct2024.csv && \
#         dvc add /opt/airflow/outputs/airflow_cleaned_data.csv && \
#         git add *.dvc dvc.yaml dvc.lock && \
#         git commit -m "Logged data automatically after Airflow pipeline"
#         """
#     )

task_trigger_model_pipeline = TriggerDagRunOperator(
        task_id="trigger_model_pipeline",
        trigger_dag_id="model_pipeline",  # The DAG id to trigger
        conf={"processed_data": "{{ task_instance.xcom_pull(task_ids='concatenate_df_target', key='df_final') }}"},
        dag=dag
    )



# Setting the task dependencies
task_participant_status_load >> task_clean_participant_status
task_demographics_load >> task_clean_demographics
[task_clean_participant_status, task_clean_demographics] >> task_merge_participant_status_and_demographics >> task_clean_participantstatus_demographic

task_biospecimen_analysis_load >> task_clean_biospecimen_analysis >> task_filter_biospecimen_analysis >> task_clean_filtered_biospecimen_analysis

[task_clean_filtered_biospecimen_analysis, task_clean_participantstatus_demographic] >> task_merge_participantstatus_demographics_biospecimen_analysis >> task_clean_participantstatus_demographics_biospecimen_analysis 
load_motor_senses_1_task >> clean_motor_senses_1_task
load_motor_senses_2_task >> clean_motor_senses_2_task
load_motor_senses_3_task >> clean_motor_senses_3_task
load_motor_senses_4_task >> clean_motor_senses_4_task
load_motor_senses_5_task >> clean_motor_senses_5_task
[clean_motor_senses_1_task,clean_motor_senses_2_task,clean_motor_senses_3_task,clean_motor_senses_4_task,clean_motor_senses_5_task]>>filter_all_motor_senses_csvs_task>>merge_all_motor_senses_csvs_task >> deduplication_motor_senses_task
[task_clean_participantstatus_demographics_biospecimen_analysis ,deduplication_motor_senses_task]>>load_and_merge_task>>seperate_target_values_task>> missing_values_drop_task>> seperate_categorical_columns_task>> seprerate_numerical_columns_task>> missing_values_impute_5percent_task>> drop_correlated_unrelated_columns_task>> missing_values_impute_50percent_scaling_task>> concatenate_df_target_task >>task_trigger_model_pipeline>>task_send_alert_email

