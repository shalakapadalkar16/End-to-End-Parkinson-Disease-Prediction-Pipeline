import pandas as pd
from sklearn.utils import resample

# Load the dataset
file_path = '/home/risa/Downloads/data_modified.csv'
data = pd.read_csv(file_path)

# Define age bins and labels, including 'Under 50'
age_bins = [0, 50, 60, 70, 80, 100]
age_labels = ['Under 50', '50-59', '60-69', '70-79', '80 and above']
data['Age_Group'] = pd.cut(data['ENROLL_AGE'], bins=age_bins, labels=age_labels, right=False)

# Analyze the distribution of participants across age groups
age_group_counts = data['Age_Group'].value_counts().sort_index()
print("Age Group Distribution:\n", age_group_counts)

# Use the size of the '70-79' age group as the reference for balancing
target_size = age_group_counts['70-79']

# Under-sample the '60-69' group to match the target size
under_sampled_60_69 = data[data['Age_Group'] == '60-69'].sample(n=target_size, random_state=1)

# Over-sample the '80 and above' group if it has fewer participants than the target size
current_count_80_above = age_group_counts['80 and above']
samples_needed = target_size - current_count_80_above
over_sampled_80_above = data[data['Age_Group'] == '80 and above'].sample(n=samples_needed, replace=True, random_state=1)

# Combine the balanced groups into a single DataFrame
balanced_data = pd.concat([
    data[data['Age_Group'] != '60-69'],  # Keep other age groups as is
    under_sampled_60_69,                 # Under-sampled '60-69' group
    over_sampled_80_above                # Over-sampled '80 and above' group
])

# Save the balanced dataset to a CSV file
balanced_data.to_csv('data_modified.csv', index=False)
print("Balanced file saved as 'balanced_data.csv'")
