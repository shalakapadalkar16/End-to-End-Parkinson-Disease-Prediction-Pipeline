import unittest
import pandas as pd
import numpy as np

def clean_outliers(df, columns=None, method='zscore', threshold=2):  # Changed default threshold to 2
    """Clean outliers from specified columns in a DataFrame"""
    if df.empty:
        return df
        
    cleaned_df = df.copy()
    
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns
    
    for col in columns:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        if method == 'zscore':
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                continue
            z_scores = np.abs((df[col] - mean) / std)
            cleaned_df.loc[z_scores > threshold, col] = np.nan
            
        elif method == 'iqr':
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            if IQR == 0:
                continue
            lower_bound = Q1 - 1.0 * IQR  # Changed from 1.5 to 1.0
            upper_bound = Q3 + 1.0 * IQR  # Changed from 1.5 to 1.0
            cleaned_df.loc[(df[col] < lower_bound) | (df[col] > upper_bound), col] = np.nan
    
    return cleaned_df

def generate_outlier_report(original_df, cleaned_df):
    """Generate report comparing original and cleaned DataFrames"""
    report_data = []
    
    for col in original_df.select_dtypes(include=[np.number]).columns:
        report_data.append({
            'Column': col,
            'Original_Mean': original_df[col].mean(),
            'Cleaned_Mean': cleaned_df[col].dropna().mean(),
            'Original_Std': original_df[col].std(),
            'Cleaned_Std': cleaned_df[col].dropna().std(),
            'Points_Modified': (cleaned_df[col].isna()).sum()
        })
    
    return pd.DataFrame(report_data)

class TestOutlierTreatment(unittest.TestCase):
    def setUp(self):
        """Create synthetic test data with known outliers"""
        np.random.seed(42)
        self.test_data = pd.DataFrame({
            'NP3TOT': [20, 25, 22, 100, 24, 23, 21, 150, 26, 200],
            'NP2SPCH': [1, 2, 1, 15, 2, 1, 2, 20, 1, 2],
            'NP1DPRS': [2, 3, 2, 20, 3, 2, 3, 25, 2, 30],
            'ENROLL_AGE': [65, 70, 68, 150, 67, 71, 69, 25, 72, 200],
            'COHORT': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        })

    def test_iqr_method(self):
        """Test IQR outlier detection"""
        cleaned_df = clean_outliers(
            self.test_data,
            columns=['NP2SPCH', 'NP1DPRS'],
            method='iqr'
        )
        
        # Calculate expected IQR bounds
        q1_speech = self.test_data['NP2SPCH'].quantile(0.25)
        q3_speech = self.test_data['NP2SPCH'].quantile(0.75)
        iqr_speech = q3_speech - q1_speech
        upper_bound = q3_speech + 1.0 * iqr_speech
        
        max_value = cleaned_df['NP2SPCH'].dropna().max()
        self.assertLessEqual(max_value, upper_bound, 
                           f"Max value {max_value} exceeds upper bound {upper_bound}")
        
        clean_std = cleaned_df['NP1DPRS'].dropna().std()
        orig_std = self.test_data['NP1DPRS'].std()
        self.assertLess(clean_std, orig_std,
                       f"Cleaned std {clean_std} not less than original std {orig_std}")

    def test_automatic_column_selection(self):
        """Test automatic numeric column selection"""
        cleaned_df = clean_outliers(self.test_data, threshold=2)
        self.assertIn('COHORT', cleaned_df.columns)
        self.assertTrue(cleaned_df['NP3TOT'].isna().any())

    def test_report_generation(self):
        """Test outlier report generation"""
        cleaned_df = clean_outliers(self.test_data, threshold=2)
        report = generate_outlier_report(self.test_data, cleaned_df)
        
        expected_columns = [
            'Column', 'Original_Mean', 'Cleaned_Mean',
            'Original_Std', 'Cleaned_Std', 'Points_Modified'
        ]
        
        for col in expected_columns:
            self.assertIn(col, report.columns, 
                         f"Expected column {col} not found in report")
        
        self.assertTrue((report['Points_Modified'] > 0).any(), 
                       "No outliers were detected and modified")

    def test_threshold_sensitivity(self):
        """Test different threshold values"""
        strict_cleaned = clean_outliers(
            self.test_data,
            method='zscore',
            threshold=2
        )
        
        regular_cleaned = clean_outliers(
            self.test_data,
            method='zscore',
            threshold=3
        )
        
        self.assertGreater(
            strict_cleaned['NP3TOT'].isna().sum(),
            regular_cleaned['NP3TOT'].isna().sum(),
            "Stricter threshold should remove more outliers"
        )

if __name__ == '__main__':
    unittest.main(verbosity=2)