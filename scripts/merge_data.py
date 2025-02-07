import os
import pandas as pd

# directory where your CSV files are stored
directory_path = '/Users/shalakapadalkar/Desktop/MLOps/Data merging/merge'

# List to hold DataFrames
dataframes = []

# Load each CSV file and add it to the list
for filename in os.listdir(directory_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(directory_path, filename)
        df = pd.read_csv(file_path)
        dataframes.append(df)

# Merge DataFrames dynamically over common columns
merged_df = dataframes[0]  # Start with the first DataFrame
for df in dataframes[1:]:
    # Merge on common columns using an outer join
    merged_df = merged_df.merge(df, on=['PATNO'], how='inner')

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged__m_output.csv', index=False)
print("Merged file saved as 'merged__m_output.csv'")