from unittest.mock import MagicMock
import unittest
import pandas as pd
from data_pipeline import (
    load_and_merge_data,
    seperate_target_values,
    missing_values_drop,
    seperate_categorical_columns,
    seprerate_numerical_columns,
    missing_values_impute_5percent,
    drop_correlated_unrelated_columns,
    missing_values_impute_50percent_scaling,
    concatenate_df_target
)

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        # Load test data from CSV
        self.test_data = pd.read_csv('test_data.csv')  # Ensure test_data.csv is in the working directory
        self.context = {'ti': MagicMock()}  # Simulating Airflow context
        self.context['ti'].xcom_pull.return_value = self.test_data

    def test_load_and_merge_data(self):
        # Call the function
        result = load_and_merge_data(self.context)

        # Check the structure of the merged data
        self.assertIn('Participant_ID', result.columns)
        self.assertIn('COHORT', result.columns)

    def test_seperate_target_values(self):
        # Call the function
        target_col, df = seperate_target_values(self.context)

        # Check that the 'COHORT' column is separated and removed
        self.assertIn('COHORT', target_col)
        self.assertNotIn('COHORT', df.columns)

    def test_missing_values_drop(self):
        # Call the function
        df = missing_values_drop(self.context)

        # Check that columns with > 50% missing values are dropped
        missing_threshold = len(df) * 0.5
        dropped_cols = [col for col in self.test_data.columns if self.test_data[col].isnull().sum() > missing_threshold]
        for col in dropped_cols:
            self.assertNotIn(col, df.columns)

    def test_seperate_categorical_columns(self):
        # Call the function
        categorical_cols = seperate_categorical_columns(self.context)

        # Check that categorical columns are correctly identified
        self.assertTrue(all(self.test_data[col].dtype == 'object' or len(self.test_data[col].unique()) < 10 for col in categorical_cols))

    def test_seprerate_numerical_columns(self):
        # Call the function
        numerical_cols = seprerate_numerical_columns(self.context)

        # Check that numerical columns are correctly identified
        self.assertTrue(all(pd.api.types.is_numeric_dtype(self.test_data[col]) for col in numerical_cols))

    def test_missing_values_impute_5percent(self):
        # Call the function
        df = missing_values_impute_5percent(self.context)

        # Check that missing values are imputed for columns with < 5% missing
        for col in self.test_data.columns:
            missing_percentage = self.test_data[col].isnull().mean()
            if missing_percentage < 0.05:
                self.assertFalse(df[col].isnull().any())

    def test_drop_correlated_unrelated_columns(self):
        # Call the function
        df, numerical_cols = drop_correlated_unrelated_columns(self.context)

        # Check that highly correlated columns or unrelated columns are dropped
        self.assertNotIn('Participant_ID', df.columns)

    def test_missing_values_impute_50percent_scaling(self):
        # Call the function
        df = missing_values_impute_50percent_scaling(self.context)

        # Check that missing values are imputed for columns with > 50% missing and scaling is applied
        for col in df.columns:
            self.assertTrue(df[col].notnull().all())

    def test_concatenate_df_target(self):
        # Mock target data
        target_data = ['Healthy', 'Sick', 'Recovered']
        self.context['ti'].xcom_pull.return_value = target_data

        # Call the function
        df_final = concatenate_df_target(self.context)

        # Check that the target column is added
        self.assertIn('COHORT', df_final.columns)
        self.assertEqual(list(df_final['COHORT']), target_data)

if __name__ == '__main__':
    unittest.main()
