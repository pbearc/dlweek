import mysql.connector
import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account

# === CONFIGURE DATABASE CONNECTION ===
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "d0a224cb89",
    "database": "calibration_db"
}

# === GOOGLE CALENDAR SETUP ===
SERVICE_ACCOUNT_FILE = "dlweek-452319-81c42245627d.json"  # Path to your JSON key file
CALENDAR_ID = "boscalibration@gmail.com"  # Google Calendar ID

# Authenticate with Google Calendar API
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/calendar"]
)
service = build("calendar", "v3", credentials=creds)

# === FUNCTION TO CREATE RECURRING EVENTS ===
def create_recurring_event(summary, start_date, interval_years):
    event = {
        "summary": summary,
        "start": {"dateTime": start_date.isoformat(), "timeZone": "UTC"},
        "end": {"dateTime": (start_date + datetime.timedelta(hours=1)).isoformat(), "timeZone": "UTC"},
        "recurrence": [f"RRULE:FREQ=YEARLY;INTERVAL={interval_years}"],
    }
    
    created_event = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
    print(f"Event created: {created_event.get('htmlLink')}")

# === FETCH DATA FROM MYSQL AND SCHEDULE EVENTS ===
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            calibration_interval, 
            calibration_due, 
            calibration_report_no,  # Fetch the calibration_report_no
            division, 
            description, 
            brand, 
            tag, 
            model_part_no, 
            serial_id_no 
        FROM calibration_data
    """)

    for row in cursor.fetchall():
        interval_years = row["calibration_interval"]
        next_calibration_date = row["calibration_due"]
        
        # Use calibration_report_no directly as the summary for the event
        summary = str(row.get("calibration_report_no", ""))  # Ensure the value is a string

        # If no calibration_report_no is provided, we can set a default or skip
        if not summary:
            print(f"Skipping event creation for record with no calibration_report_no.")
            continue

        if next_calibration_date:
            create_recurring_event(
                summary=summary,  # Use the calibration_report_no as the summary
                start_date=next_calibration_date,
                interval_years=interval_years
            )

    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"Error: {err}")
