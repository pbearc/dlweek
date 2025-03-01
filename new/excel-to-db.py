import pandas as pd
from sqlalchemy import create_engine, text

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

# Insert data into tools table
tools_df = df[["Serial_Id_No", "Division", "Description", "Brand", "Tag", "Model_Part_No", "Measurement_Range", "Tolerance_Limit_External", "Tolerance_Limit_Internal"]]
tools_df.to_sql("tools", con=engine, if_exists="replace", index=False)

# Insert data into calibration table
calibration_df = df[["Serial_Id_No", "Last_Calibration", "Calibration_Due", "Remaining_Months", "External_Internal", "Calibration_Report_No", "Calibrator"]]
calibration_df = calibration_df.rename(columns={
    "Last_Calibration": "last_calibration_date",
    "Calibration_Due": "next_due_date",
    "External_Internal": "external_internal",
    "Calibration_Report_No": "report_number"
})
calibration_df["lab"] = df["External_Internal"].apply(lambda x: x.split()[0] if pd.notna(x) else None)
calibration_df.to_sql("calibration", con=engine, if_exists="replace", index=False)

# Insert data into toolusage table
toolusage_df = df[["Serial_Id_No", "In_Use", "Calibration_Interval"]]
toolusage_df["in_use"] = toolusage_df["In_Use"].map({"Yes": True, "No": False})
toolusage_df = toolusage_df.rename(columns={"Calibration_Interval": "actual_interval_years"})
toolusage_df = toolusage_df[["Serial_Id_No", "in_use", "actual_interval_years"]]
toolusage_df.to_sql("toolusage", con=engine, if_exists="replace", index=False)

print("✅ Data successfully inserted into MySQL tables!")
