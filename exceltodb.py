import pandas as pd
from sqlalchemy import create_engine

# Load Excel file
df = pd.read_excel("calibration_data.xlsx")

# Rename columns to match MySQL table
df.columns = [
    "Division", "Description", "Brand", "Tag", "Model_Part_No", "Serial_Id_No",
    "Measurement_Range", "Tolerance_Limit_External", "Tolerance_Limit_Internal", "In_Use", "Calibration_Interval",
    "Last_Calibration", "Calibration_Due", "Remaining_Months", "External_Internal",
    "Calibration_Report_No", "Calibrator", "PIC", "Renewal_Action"
]

# Convert date columns to proper format
df["Last_Calibration"] = pd.to_datetime(df["Last_Calibration"], errors="coerce")
df["Calibration_Due"] = pd.to_datetime(df["Calibration_Due"], errors="coerce")

# Connect to MySQL
engine = create_engine("mysql+pymysql://root:d0a224cb89@localhost:3306/calibration_db")

# Insert data into MySQL
df.to_sql("calibration_data", con=engine, if_exists="replace", index=False)

print("✅ Data successfully inserted into MySQL!")
