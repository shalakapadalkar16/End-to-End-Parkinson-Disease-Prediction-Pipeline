# Project Overview
This project aims to predict Parkinson's disease using machine learning algorithms and MLOPS techniques. By analyzing biomedical data such as demographic attributes, motor skills, and other relevant biomarkers this model can assist in early identification of Parkinson's disease symptoms. Parkinson's disease is a progressive neurological disorder with no known cure, but early detection can significantly improve patient outcomes by enabling earlier interventions. Predictive models help in identifying the disease at an early stage when treatments can be more effective in managing symptoms, thereby improving the quality of life for affected individuals. This project supports healthcare professionals by providing a tool for early detection, which can aid in timely diagnosis and treatment planning. Additionally, it can be a valuable resource for researchers studying Parkinson's disease, potentially contributing to the discovery of new biomarkers or insights into disease progression. 

## Environment Setup
python3 -m venv env
source env/bin/activate  # On Windows, use `env\Scripts\activate`

## Installing dependencies
pip install -r requirements.txt </br>
The requirements.txt includes essential packages like pandas, numpy, scikit-learn, tensorflow, matplotlib,airflow and others used in the pipeline.

### Prerequisites
To run this project, you need:
- Python 3.8 or above
- `pip` for package management

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/mrudulaacharya/Parkinson-s-Prediction.git
   cd Parkinson-s-Prediction
2.	Download and install docker from official website
3.	Update the docker-compose file:
4.	Paths to the folders where csv files are mountedb.	Email for alert notifications
5.	To build the docker services: `docker-compose build`
6.	To run the services: `docker-compose up`
7.	To stop the services: `docker-compose down`
8.	To clean up all stopped containers, networks, and volumes created by Docker Compose: `docker-compose down –volumes`
9.	To access the airflow web UI: open http://localhost:8080 in your browser.
10.	Trigger both DAGs: data_pipeline and model_pipeline
11.	To view the model parameters, accuracy metrics like F-1 score, model metadata: paste 127.0.0.1:5000 in the browser


## Dataset
The dataset is sourced from the Parkinson's Progression Markers Initiative (PPMI) and comprises comprehensive biomedical data, including demographic details, motor assessments, and various biomarkers pertinent to Parkinson's disease.


## Description of the Data Pipeline Components:
• send_custom_alert_email: Sends a custom alert email if a task fails or is retried, with details about the task and DAG.  
• participant_status_load: Loads the "Participant_Status" CSV file into a DataFrame.  
• demographics_load: Loads the "Demographics" CSV file into a DataFrame.  
• clean_participant_status: Cleans the "Participant_Status" DataFrame by converting enrollment dates, renaming a column, and dropping unnecessary columns.  
• merge_participant_status_and_demographics: Merges the cleaned "Participant_Status" and "Demographics" DataFrames on the participant ID and filters rows with valid enrollment statuses.  
• clean_demographics: Cleans the "Demographics" DataFrame by dropping columns that are not needed.
• clean_participantstatus_demographic: Further cleans the merged "Participant_Status" and "Demographics" DataFrame by dropping additional unnecessary columns.  
• biospecimen_analysis_load: Loads the "SAA_Biospecimen_Analysis_Results" CSV file into a DataFrame.  
• clean_biospecimen_analysis: Cleans the "Biospecimen_Analysis" DataFrame by formatting dates and dropping irrelevant columns.  
• filter_biospecimen_analysis: Filters the "Biospecimen_Analysis" DataFrame to keep only records with the earliest run date for baseline clinical events.  
• clean_filtered_biospecimen_analysis: Further cleans the filtered "Biospecimen_Analysis" DataFrame by dropping additional columns.  
• merge_participantstatus_demographics_biospecimen_analysis: Merges the cleaned "Participant_Status", "Demographics", and "Biospecimen_Analysis" DataFrames.  
• clean_participantstatus_demographics_biospecimen_analysis: Final cleanup of the merged DataFrame by dropping remaining unnecessary columns.  
• load_motor_senses_1: Loads the first motor senses CSV file into a DataFrame.  
• load_motor_senses_2: Loads the second motor senses CSV file into a DataFrame.  
• load_motor_senses_3: Loads the third motor senses CSV file into a DataFrame.  
• load_motor_senses_4: Loads the fourth motor senses CSV file into a DataFrame.  
• load_motor_senses_5: Loads the fifth motor senses CSV file into a DataFrame.  
• clean_motor_senses_1: Cleans the first motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_2: Cleans the second motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_3: Cleans the third motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_4: Cleans the fourth motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_5: Cleans the fifth motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• merge_all_motor_senses_csvs: Merges all cleaned motor senses DataFrames into a single DataFrame and pushes the merged DataFrame to XCom.  
• drop_duplicate_motor_senses_columns: Removes duplicate columns from the merged DataFrame and saves the final deduplicated DataFrame to a CSV file.  


## Airflow DAG components
![WhatsApp Image 2024-11-05 at 23 45 16_95809d38](https://github.com/user-attachments/assets/594b4ec5-9ee6-417f-8f67-6e16da2f5f2f)
![WhatsApp Image 2024-11-05 at 23 43 29_8baaf474](https://github.com/user-attachments/assets/cccacb79-1bf9-4546-8728-4b093913605e)

