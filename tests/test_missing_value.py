
import unittest
import pandas as pd
import numpy as np

def handle_missing_values(data, identify_only=False, drop_threshold=None, strategy=None, constant_value=None):
    """
    Handle missing values in a pandas DataFrame using various strategies.
    """
    if not isinstance(data, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")

    # Count missing values per column
    missing_counts = data.isna().sum().to_dict()
    
    if identify_only:
        return missing_counts
    
    # Create a copy to avoid modifying original data
    df = data.copy()
    
    # Drop columns based on threshold or all missing values
    if drop_threshold is not None:
        total_rows = len(df)
        columns_to_drop = [col for col, count in missing_counts.items() 
                          if count/total_rows > drop_threshold or count == total_rows]
        df = df.loc[:, ~df.columns.isin(columns_to_drop)]
    else:
        # Only drop columns with all missing values if no threshold is specified
        df = df.dropna(axis=1, how='all')
    
    # Handle custom strategies
    if strategy is not None:
        if not isinstance(strategy, dict):
            raise ValueError("Strategy must be a dictionary")
            
        for column, method in strategy.items():
            if column not in df.columns:
                continue
                
            if method == 'median':
                df[column] = df[column].fillna(df[column].median())
            elif method == 'constant' and constant_value is not None:
                df[column] = df[column].fillna(constant_value)
            elif callable(method):
                df[column] = method(df[column])
            else:
                raise ValueError(f"Invalid strategy '{method}' for column '{column}'")
    else:
        # Default handling
        for column in df.columns:
            if df[column].dtype.kind in 'iuf':  # integer or float
                df[column] = df[column].fillna(df[column].mean())
            else:  # categorical/object
                df[column] = df[column].fillna(df[column].mode()[0])
    
    return df

class TestMissingValuesHandler(unittest.TestCase):
    def setUp(self):
        # Create a sample dataset with various types of missing values
        self.data = pd.DataFrame({
            'AV133STDY': [1.0, np.nan, 3.0, np.nan, 5.0],
            'TAUSTDY': [np.nan, 2.0, np.nan, 4.0, 5.0],
            'GAITSTDY': [1.0, 2.0, 3.0, 4.0, np.nan],
            'PISTDY': [np.nan, np.nan, np.nan, np.nan, np.nan],
            'NP4DYSTN': [1, 2, np.nan, 4, 5],
            'NP4TOT': [10, np.nan, 30, np.nan, 50]
        })

    def test_missing_values_identification(self):
        # Test if the function correctly identifies missing values
        missing_values = handle_missing_values(self.data, identify_only=True)
        self.assertEqual(missing_values['AV133STDY'], 2)
        self.assertEqual(missing_values['TAUSTDY'], 2)
        self.assertEqual(missing_values['GAITSTDY'], 1)
        self.assertEqual(missing_values['PISTDY'], 5)
        self.assertEqual(missing_values['NP4DYSTN'], 1)
        self.assertEqual(missing_values['NP4TOT'], 2)

    def test_missing_values_handling(self):
        # Test if the function correctly handles missing values
        handled_data = handle_missing_values(self.data)
        
        # Check if numerical columns are imputed with mean
        self.assertAlmostEqual(handled_data['AV133STDY'].mean(), 3.0)
        self.assertAlmostEqual(handled_data['TAUSTDY'].mean(), 3.67,places=2)
        self.assertAlmostEqual(handled_data['GAITSTDY'].mean(), 2.5)
        
        # Check if columns with all missing values are dropped
        self.assertNotIn('PISTDY', handled_data.columns)
        
        # Check if categorical columns are imputed with mode
        self.assertTrue(handled_data['NP4DYSTN'].notna().all())
        
        # Check if NP4TOT is imputed correctly
        self.assertTrue(handled_data['NP4TOT'].notna().all())

    def test_threshold_dropping(self):
        # Test if columns with more than 50% missing values are dropped
        handled_data = handle_missing_values(self.data, drop_threshold=0.5)
        self.assertNotIn('PISTDY', handled_data.columns)
        self.assertIn('AV133STDY', handled_data.columns)

    def test_custom_strategy(self):
        # Test if custom imputation strategy works
        custom_strategy = {
            'AV133STDY': 'median',
            'TAUSTDY': 'constant',
            'GAITSTDY': lambda x: x.fillna(x.min())
        }
        handled_data = handle_missing_values(self.data, strategy=custom_strategy, constant_value=100)
        self.assertEqual(handled_data['AV133STDY'].median(), 3.0)
        self.assertEqual(handled_data['TAUSTDY'].fillna(100).unique().tolist(), [100.0, 2.0, 4.0, 5.0])
        self.assertEqual(handled_data['GAITSTDY'].min(), 1.0)

    def test_error_handling(self):
        # Test if the function raises appropriate errors
        with self.assertRaises(ValueError):
            handle_missing_values(self.data, strategy='invalid_strategy')
        with self.assertRaises(TypeError):
            handle_missing_values('not_a_dataframe')

if __name__ == '__main__':
    unittest.main()