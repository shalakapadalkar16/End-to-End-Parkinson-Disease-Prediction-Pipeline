import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import os

# Assuming your functions are in a module named 'motor_senses_module'
from motor_senses_module import (
    load_motor_senses_1, load_motor_senses_2, load_motor_senses_3, load_motor_senses_4, load_motor_senses_5,
    clean_motor_senses_1, clean_motor_senses_2, clean_motor_senses_3, clean_motor_senses_4, clean_motor_senses_5,
    filter_all_motor_senses_csvs, merge_all_motor_senses_csvs, drop_duplicate_motor_senses_columns
)

class TestMotorSensesFunctions(unittest.TestCase):

    @patch('motor_senses_module.pd.read_csv')
    def test_load_motor_senses(self, mock_read_csv):
        # Mocking the pandas read_csv function
        mock_read_csv.return_value = pd.DataFrame({
            'PATNO': [1, 2],
            'MDS_UPDRS': [10, 20]
        })
        
        context = {'ti': MagicMock()}
        result = load_motor_senses_1(**context)
        
        mock_read_csv.assert_called_once_with(os.path.join('your_directory', 'MDS-UPDRS_Part_I_27Oct2024.csv'))
        pd.testing.assert_frame_equal(result, pd.DataFrame({'PATNO': [1, 2], 'MDS_UPDRS': [10, 20]}))

    @patch('motor_senses_module.pd.read_csv')
    def test_clean_motor_senses_1(self, mock_read_csv):
        mock_df = pd.DataFrame({
            'PATNO': [1, 2],
            'REC_ID': [101, 102],
            'PAG_NAME': ['A', 'B'],
            'MDS_UPDRS': [10, 20]
        })
        context = {'ti': MagicMock()}
        context['ti'].xcom_pull.return_value = mock_df
        
        result = clean_motor_senses_1(**context)
        
        expected_df = pd.DataFrame({
            'PATNO': [1, 2],
            'MDS_UPDRS': [10, 20]
        })
        
        pd.testing.assert_frame_equal(result, expected_df)
        context['ti'].xcom_push.assert_called_once_with(key='cleaned_1', value=expected_df)

    @patch('motor_senses_module.pd.read_csv')
    def test_clean_motor_senses_2(self, mock_read_csv):
        mock_df = pd.DataFrame({
            'PATNO': [1, 2],
            'REC_ID': [101, 102],
            'PAG_NAME': ['A', 'B'],
            'MDS_UPDRS': [10, 20]
        })
        context = {'ti': MagicMock()}
        context['ti'].xcom_pull.return_value = mock_df
        
        result = clean_motor_senses_2(**context)
        
        expected_df = pd.DataFrame({
            'PATNO': [1, 2],
            'MDS_UPDRS': [10, 20]
        })
        
        pd.testing.assert_frame_equal(result, expected_df)
        context['ti'].xcom_push.assert_called_once_with(key='cleaned_2', value=expected_df)

    def test_filter_all_motor_senses_csvs(self):
        # Mocking cleaned motor senses data
        mock_cleaned_dfs = [
            pd.DataFrame({'PATNO': [1, 2], 'ORIG_ENTRY': ['01/2020', '02/2020'], 'MDS_UPDRS': [10, 20]}),
            pd.DataFrame({'PATNO': [1, 2], 'ORIG_ENTRY': ['01/2020', '03/2020'], 'MDS_UPDRS': [15, 25]})
        ]
        
        context = {'ti': MagicMock()}
        context['ti'].xcom_pull.side_effect = mock_cleaned_dfs
        
        filter_all_motor_senses_csvs(**context)
        
        # Ensure merged data is pushed to XCom
        context['ti'].xcom_push.assert_called_once_with(key='filtered_dfs', value=mock_cleaned_dfs)

    def test_merge_all_motor_senses_csvs(self):
        # Mocking filtered dataframes to be merged
        mock_filtered_dfs = [
            pd.DataFrame({'PATNO': [1, 2], 'MDS_UPDRS': [10, 20]}),
            pd.DataFrame({'PATNO': [1, 2], 'MDS_UPDRS': [15, 25]})
        ]
        
        context = {'ti': MagicMock()}
        context['ti'].xcom_pull.return_value = mock_filtered_dfs
        
        merge_all_motor_senses_csvs(**context)
        
        # Check that the merge is happening properly
        merged_df = mock_filtered_dfs[0].merge(mock_filtered_dfs[1], on='PATNO', how='outer')
        context['ti'].xcom_push.assert_called_once_with(key='filter_merged_df', value=merged_df)

    def test_drop_duplicate_motor_senses_columns(self):
        # Mocking merged DataFrame
        mock_merged_df = pd.DataFrame({
            'PATNO': [1, 2],
            'MDS_UPDRS': [10, 20],
            'EVENT_ID_dup': ['A', 'B'],
            'NUPSOURC_dup': ['X', 'Y']
        })
        
        context = {'ti': MagicMock()}
        context['ti'].xcom_pull.return_value = mock_merged_df
        
        result = drop_duplicate_motor_senses_columns(**context)
        
        # Expected deduplicated dataframe
        expected_df = pd.DataFrame({
            'PATNO': [1, 2],
            'MDS_UPDRS': [10, 20]
        })
        
        pd.testing.assert_frame_equal(result, expected_df)
        context['ti'].xcom_push.assert_called_once_with(key='deduped_df', value=expected_df)

if __name__ == "__main__":
    unittest.main()
