import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from data_pipeline import (
    participant_status_load,
    clean_participant_status,
    merge_participant_status_and_demographics,
    demographics_load,
    clean_demographics,
    merge_demographics_and_biospecimen,
    biospecimen_load,
    clean_biospecimen,
    merge_biospecimen_and_status
)

class TestDataProcessing(unittest.TestCase):

    @patch("pandas.read_csv")
    def test_participant_status_load(self, mock_read_csv):
        # Arrange: Mock the CSV data
        mock_data = pd.DataFrame({
            "PATNO": [1, 2],
            "ENROLL_DATE": ["01/2020", "02/2020"],
            "ENROLL_STATUS": ["Enrolled", "Complete"]
        })
        mock_read_csv.return_value = mock_data

        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = participant_status_load(**{"context": mock_context})

        # Assert: Verify that data was loaded and pushed to XCom
        mock_read_csv.assert_called_once_with('participant_status_path')  # Assuming 'participant_status_path' is the path
        mock_context['ti'].xcom_push.assert_called_once_with(key='participant_status', value=mock_data)

        # Verify the returned DataFrame is correct
        pd.testing.assert_frame_equal(result, mock_data)

    @patch("pandas.read_csv")
    def test_clean_participant_status(self, mock_read_csv):
        # Mock the input data from XCom
        input_data = pd.DataFrame({
            "PATNO": [1, 2],
            "ENROLL_DATE": ["01/2020", "02/2020"],
            "ENROLL_STATUS": ["Enrolled", "Complete"],
            "COHORT_DEFINITION": [None, None],
            "STATUS_DATE": [None, None]
        })
        
        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_pull.return_value = input_data
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = clean_participant_status(**{"context": mock_context})

        # Assert: Verify the transformation
        mock_context['ti'].xcom_push.assert_called_once_with(key='cleaned_participant_status', value=result)
        
        # Assert the resulting DataFrame has the correct transformations (like renamed columns and dropped columns)
        expected_result = pd.DataFrame({
            "Participant_ID": [1, 2],
            "ENROLL_DATE": pd.to_datetime(["01/2020", "02/2020"]),
            "ENROLL_STATUS": ["Enrolled", "Complete"]
        })

        pd.testing.assert_frame_equal(result, expected_result)

    def test_merge_participant_status_and_demographics(self):
        # Mock participant status and demographics DataFrames
        participant_status = pd.DataFrame({
            "Participant_ID": [1, 2],
            "ENROLL_STATUS": ["Enrolled", "Complete"],
            "ENROLL_DATE": pd.to_datetime(["01/2020", "02/2020"])
        })
        demographics = pd.DataFrame({
            "PATNO": [1, 2],
            "SEX": ["M", "F"],
            "AGE": [65, 70]
        })

        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_pull.side_effect = [participant_status, demographics]
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = merge_participant_status_and_demographics(**{"context": mock_context})

        # Assert: Check that the merge happened correctly
        expected_result = pd.DataFrame({
            "Participant_ID": [1, 2],
            "ENROLL_STATUS": ["Enrolled", "Complete"],
            "ENROLL_DATE": pd.to_datetime(["01/2020", "02/2020"]),
            "PATNO": [1, 2],
            "SEX": ["M", "F"],
            "AGE": [65, 70]
        })

        pd.testing.assert_frame_equal(result, expected_result)

    @patch("pandas.read_csv")
    def test_demographics_load(self, mock_read_csv):
        # Mock the CSV data
        mock_data = pd.DataFrame({
            "PATNO": [1, 2],
            "SEX": ["M", "F"],
            "AGE": [65, 70]
        })
        mock_read_csv.return_value = mock_data

        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = demographics_load(**{"context": mock_context})

        # Assert: Verify that data was loaded and pushed to XCom
        mock_read_csv.assert_called_once_with('demographics_path')  # Assuming 'demographics_path' is the path
        mock_context['ti'].xcom_push.assert_called_once_with(key='demographics', value=mock_data)

        # Verify the returned DataFrame is correct
        pd.testing.assert_frame_equal(result, mock_data)

    def test_clean_demographics(self):
        # Mock the input data from XCom
        input_data = pd.DataFrame({
            "PATNO": [1, 2],
            "SEX": ["M", "F"],
            "AGE": [65, 70]
        })
        
        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_pull.return_value = input_data
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = clean_demographics(**{"context": mock_context})

        # Assert: Verify the transformation
        mock_context['ti'].xcom_push.assert_called_once_with(key='cleaned_demographics', value=result)
        
        # Assert the resulting DataFrame is unchanged as there was no transformation
        pd.testing.assert_frame_equal(result, input_data)

    @patch("pandas.read_csv")
    def test_biospecimen_load(self, mock_read_csv):
        # Mock the CSV data
        mock_data = pd.DataFrame({
            "PATNO": [1, 2],
            "SAAStatus": ["Normal", "Abnormal"],
            "SAAType": ["Type A", "Type B"]
        })
        mock_read_csv.return_value = mock_data

        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = biospecimen_load(**{"context": mock_context})

        # Assert: Verify that data was loaded and pushed to XCom
        mock_read_csv.assert_called_once_with('biospecimen_path')  # Assuming 'biospecimen_path' is the path
        mock_context['ti'].xcom_push.assert_called_once_with(key='biospecimen', value=mock_data)

        # Verify the returned DataFrame is correct
        pd.testing.assert_frame_equal(result, mock_data)

    def test_clean_biospecimen(self):
        # Mock the input data from XCom
        input_data = pd.DataFrame({
            "PATNO": [1, 2],
            "SAAStatus": ["Normal", "Abnormal"],
            "SAAType": ["Type A", "Type B"]
        })
        
        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_pull.return_value = input_data
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = clean_biospecimen(**{"context": mock_context})

        # Assert: Verify the transformation (in this case, no changes to the data)
        mock_context['ti'].xcom_push.assert_called_once_with(key='cleaned_biospecimen', value=result)
        
        # Assert the resulting DataFrame is unchanged as there was no transformation
        pd.testing.assert_frame_equal(result, input_data)

    def test_merge_biospecimen_and_status(self):
        # Mock biospecimen and status DataFrames
        biospecimen = pd.DataFrame({
            "PATNO": [1, 2],
            "SAAStatus": ["Normal", "Abnormal"],
            "SAAType": ["Type A", "Type B"]
        })
        participant_status = pd.DataFrame({
            "Participant_ID": [1, 2],
            "ENROLL_STATUS": ["Enrolled", "Complete"],
            "ENROLL_DATE": pd.to_datetime(["01/2020", "02/2020"])
        })

        # Mock the context
        mock_context = MagicMock()
        mock_context['ti'].xcom_pull.side_effect = [biospecimen, participant_status]
        mock_context['ti'].xcom_push = MagicMock()

        # Act: Call the function
        result = merge_biospecimen_and_status(**{"context": mock_context})

        # Assert: Check that the merge happened correctly
        expected_result = pd.DataFrame({
            "PATNO": [1, 2],
            "SAAStatus": ["Normal", "Abnormal"],
            "SAAType": ["Type A", "Type B"],
            "Participant_ID": [1, 2],
            "ENROLL_STATUS": ["Enrolled", "Complete"],
            "ENROLL_DATE": pd.to_datetime(["01/2020", "02/2020"])
        })

        pd.testing.assert_frame_equal(result, expected_result)

if __name__ == "__main__":
    unittest.main()
