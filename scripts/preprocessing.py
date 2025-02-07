import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest


def clean_outliers(df, columns=None, method='zscore', threshold=3):
    df_clean = df.copy()
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns

    def zscore_method(data):
        z_scores = np.abs(stats.zscore(data, nan_policy='omit'))
        return z_scores < threshold

    def iqr_method(data):
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (data >= lower_bound) & (data <= upper_bound)

    for column in columns:
        if df_clean[column].dtype in [np.number]:
            if method == 'zscore':
                mask = zscore_method(df_clean[column])
                df_clean.loc[~mask, column] = df_clean[column].median()
            elif method == 'iqr':
                mask = iqr_method(df_clean[column])
                df_clean.loc[~mask, column] = df_clean[column].median()
            elif method == 'isolation_forest':
                iso_forest = IsolationForest(contamination=0.1, random_state=42)
                yhat = iso_forest.fit_predict(df_clean[column].values.reshape(-1, 1))
                mask = yhat != -1
                df_clean.loc[~mask, column] = df_clean[column].median()
    return df_clean

# Function 1: Data Cleaning

def clean_data(df):
    # Drop duplicate rows
    df = df.drop_duplicates()
    # Drop columns with more than 90% missing values
    drop_threshold = 0.6 * len(df)
    missing_values = df.isnull().sum()
    columns_to_drop = missing_values[missing_values > drop_threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    
    # Handle missing values
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            # Fill numerical columns with mean
            df[col].fillna(int(df[col].mean()), inplace=True)
        elif df[col].dtype == 'object':
            # Fill categorical columns with mode
            df[col].fillna(df[col].mode()[0], inplace=True)

    columns_to_treat = [
        'ENROLL_AGE', 'NP3TOT', 'NP1DPRS', 'NP1ANXS', 'NP1FATG',
        'NP2SPCH', 'NP2WALK', 'NP3GAIT', 'NP3BRADY'
    ]

    # Step 4: Treat outliers using clean_outliers function
    df = clean_outliers(df, columns=columns_to_treat, method='isolation_forest')
    
    print("Data cleaning complete. Missing values handled and duplicates dropped.")
    df.to_csv('/home/yutika/MLops_project/Parkinson-s-Prediction/scripts/cleaned_data.csv', index=False)

    return df

# Function 2: Data Preprocessing
def preprocess_data(df):
    # Convert date columns to datetime format if any
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # One-hot encode categorical variables
    categorical_cols = df.select_dtypes(include=['object']).columns
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
    
    print("Data preprocessing complete. Date conversion and encoding done.")
    return df

# Function 3: EDA for ENROLL_AGE, SEX, and COHORT columns
def eda(df):
    # Distribution of ENROLL_AGE
    if 'ENROLL_AGE' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.histplot(df['ENROLL_AGE'], kde=True, bins=20)
        plt.title('Distribution of ENROLL_AGE')
        plt.xlabel('ENROLL_AGE')
        plt.ylabel('Frequency')
        plt.show()
    else:
        print("ENROLL_AGE column not found in the dataset.")
    
    # Count plot for SEX
    if 'SEX' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.countplot(x='SEX', data=df)
        plt.title('Distribution of SEX')
        plt.xlabel('SEX')
        plt.ylabel('Count')
        plt.show()
    else:
        print("SEX column not found in the dataset.")
    
    # Count plot for COHORT
    if 'COHORT' in df.columns:
        plt.figure(figsize=(8, 4))
        sns.countplot(x='COHORT', data=df)
        plt.title('Distribution of COHORT')
        plt.xlabel('COHORT')
        plt.ylabel('Count')
        plt.show()
    else:
        print("COHORT column not found in the dataset.")

file_path = '/home/yutika/Downloads/modified_dataset.csv' 
df = pd.read_csv(file_path)

# Step 1: Clean the data
df_cleaned = clean_data(df)

# Step 2: Preprocess the data
df_preprocessed = preprocess_data(df_cleaned)

# Step 3: Perform EDA for ENROLL_AGE, SEX, and COHORT
eda(df_preprocessed)
