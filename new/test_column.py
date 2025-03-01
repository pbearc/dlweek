import pandas as pd

file_path = "calibration_data.xlsx"  # Ensure correct path
df = pd.read_excel(file_path, dtype=str)  # Read as string to avoid type issues

# Print the original column names
print("Original Column Names:", df.columns.tolist())
