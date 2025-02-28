import streamlit as st
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import google.oauth2.service_account
from googleapiclient.discovery import build
import pytz
from streamlit_calendar import calendar
import streamlit.components.v1 as components

# === GOOGLE CALENDAR CONFIGURATION ===
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
SERVICE_ACCOUNT_FILE = 'dlweek-452319-81c42245627d.json'  # Replace with your actual path

# === CONFIGURE DATABASE CONNECTION ===
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "d0a224cb89",
    "database": "calibration_db"
}

# === GOOGLE CALENDAR HELPER FUNCTIONS ===
def get_calendar_events(service, calendar_id, timezone):
    now = datetime.utcnow()
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)

    try:
        events_result = service.events().list(
            calendarId=calendar_id,
            timeMin=start_of_month.isoformat() + 'Z',
            timeMax=end_of_month.isoformat() + 'Z',
            singleEvents=True,
            orderBy='startTime'
        ).execute()

        events = events_result.get('items', [])
        formatted_events = []

        for event in events:
            start = event['start'].get('dateTime', event['start'].get('date'))
            end = event['end'].get('dateTime', event['end'].get('date'))
            summary = event['summary']

            if start:
                start_datetime = datetime.fromisoformat(start[:-1]) if start.endswith('Z') else datetime.fromisoformat(start)
                local_start_datetime = start_datetime.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(timezone))
                start_formatted = local_start_datetime.isoformat()
            else:
                start_formatted = None

            if end:
                end_datetime = datetime.fromisoformat(end[:-1]) if end.endswith('Z') else datetime.fromisoformat(end)
                local_end_datetime = end_datetime.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(timezone))
                end_formatted = local_end_datetime.isoformat()
            else:
                end_formatted = None

            formatted_events.append({
                'title': summary,
                'start': start_formatted,
                'end': end_formatted,
                'allDay': 'date' in event['start']
            })

        return formatted_events

    except Exception as e:
        st.error(f"Error fetching calendar events: {e}")
        return []

def display_calendar(events):
    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek,timeGridDay"
        },
        "initialView": "dayGridMonth",
        "selectable": True,
        "editable": False,
        "events": events
    }

    calendar(events=events, options=calendar_options)

def display_calendar_page(calendar_id, timezone):
    creds = google.oauth2.service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('calendar', 'v3', credentials=creds)

    st.title("Calibration Calendar")

    events = get_calendar_events(service, calendar_id, timezone)
    display_calendar(events)

    calendar_url = f"https://calendar.google.com/calendar/embed?src={calendar_id}"
    st.markdown(f"[Open in Google Calendar]({calendar_url})", unsafe_allow_html=True)

# === DATABASE HELPER FUNCTIONS ===
def get_calibration_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM calibration_data")
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Error as e:
        st.error(f"Error: {e}")
        return []

def add_calibration_data(data):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO calibration_data (Division, Description, Brand, Tag, Model_Part_No, Serial_Id_No, Measurement_Range, 
                                         Tolerance_Limit_External, Tolerance_Limit_Internal, In_Use, Calibration_Interval, 
                                         Last_Calibration, Calibration_Due, Remaining_Months, External_Internal, Calibration_Report_No, 
                                         Calibrator, PIC, Renewal_Reminder)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data['division'], data['description'], data['brand'], data['tag'], data['model_part_no'], data['serial_id_no'],
              data['measurement_range'], data['tolerance_limit_external'], data['tolerance_limit_internal'],
              data['in_use'], data['calibration_interval'], data['last_calibration'], data['calibration_due'],
              data['remaining_months'], data['external_internal'], data['calibration_report_no'], data['calibrator'],
              data['pic'], data['renewal_reminder']))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("Calibration data added successfully!")
    except Error as e:
        st.error(f"Error: {e}")

def delete_calibration_data(id):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM calibration_data WHERE calibration_report_no = %s", (id,))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("Calibration data deleted successfully!")
    except Error as e:
        st.error(f"Error: {e}")

def update_calibration_data(id, data):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE calibration_data
            SET Division = %s, Description = %s, Brand = %s, Tag = %s, Model_Part_No = %s, Serial_Id_No = %s, Measurement_Range = %s,
                Tolerance_Limit_External = %s, Tolerance_Limit_Internal = %s, In_Use = %s, Calibration_Interval = %s, 
                Last_Calibration = %s, Calibration_Due = %s, Remaining_Months = %s, External_Internal = %s, Calibration_Report_No = %s, 
                Calibrator = %s, PIC = %s, Renewal_Reminder = %s
            WHERE calibration_report_no = %s
        """, (data['division'], data['description'], data['brand'], data['tag'], data['model_part_no'], data['serial_id_no'],
              data['measurement_range'], data['tolerance_limit_external'], data['tolerance_limit_internal'], data['in_use'],
              data['calibration_interval'], data['last_calibration'], data['calibration_due'], data['remaining_months'],
              data['external_internal'], data['calibration_report_no'], data['calibrator'], data['pic'], data['renewal_reminder'],
              id))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("Calibration data updated successfully!")
    except Error as e:
        st.error(f"Error: {e}")

# === STREAMLIT UI FUNCTIONS ===
def calibration_management_page():
    st.title("Calibration Management")

    if st.button("Show All Calibration Data"):
        calibration_data = get_calibration_data()
        st.write(calibration_data)

    st.header("Add New Calibration")
    with st.form(key='add_form'):
        division = st.text_input("Division")
        description = st.text_input("Description")
        brand = st.text_input("Brand")
        tag = st.text_input("Tag")
        model_part_no = st.text_input("Model Part No")
        serial_id_no = st.text_input("Serial ID No")
        measurement_range = st.text_input("Measurement Range")
        tolerance_limit_external = st.text_input("Tolerance Limit External")
        tolerance_limit_internal = st.text_input("Tolerance Limit Internal")
        in_use = st.selectbox("In Use", ["Yes", "No"])
        calibration_interval = st.number_input("Calibration Interval (years)", min_value=1, value=2)
        last_calibration = st.date_input("Last Calibration Date", min_value=datetime(2000, 1, 1))
        calibration_due = st.date_input("Calibration Due Date", min_value=datetime(2000, 1, 1))
        remaining_months = st.number_input("Remaining Months", min_value=0, value=12)
        external_internal = st.selectbox("External/Internal", ["External", "Internal"])
        calibration_report_no = st.text_input("Calibration Report No")
        calibrator = st.text_input("Calibrator")
        pic = st.text_input("PIC")
        renewal_reminder = st.text_input("Renewal Reminder")
        submit_button = st.form_submit_button("Add Calibration")
        if submit_button:
            data = {
                "division": division, "description": description, "brand": brand, "tag": tag,
                "model_part_no": model_part_no,
                "serial_id_no": serial_id_no, "measurement_range": measurement_range,
                "tolerance_limit_external": tolerance_limit_external,
                "tolerance_limit_internal": tolerance_limit_internal, "in_use": in_use,
                "calibration_interval": calibration_interval,
                "last_calibration": last_calibration, "calibration_due": calibration_due,
                "remaining_months": remaining_months,
                "external_internal": external_internal, "calibration_report_no": calibration_report_no,
                "calibrator": calibrator,
                "pic": pic, "renewal_reminder": renewal_reminder
            }
            add_calibration_data(data)

    st.header("Delete Calibration")
    with st.form(key='delete_form'):
        delete_id = st.number_input("Enter Calibration Report Number to Delete", min_value=1)
        delete_button = st.form_submit_button("Delete Calibration")
        if delete_button:
            delete_calibration_data(delete_id)

    st.header("Update Calibration")
    with st.form(key='update_form'):
        update_id = st.number_input("Enter Calibration Report Number to Update", min_value=1)
        division = st.text_input("Division")
        description = st.text_input("Description")
        brand = st.text_input("Brand")
        tag = st.text_input("Tag")
        model_part_no = st.text_input("Model Part No")
        serial_id_no = st.text_input("Serial ID No")
        measurement_range = st.text_input("Measurement Range")
        tolerance_limit_external = st.text_input("Tolerance Limit External")
        tolerance_limit_internal = st.text_input("Tolerance Limit Internal")
        in_use = st.selectbox("In Use", ["Yes", "No"])
        calibration_interval = st.number_input("Calibration Interval (years)", min_value=1, value=2)
        last_calibration = st.date_input("Last Calibration Date", min_value=datetime(2000, 1, 1))
        calibration_due = st.date_input("Calibration Due Date", min_value=datetime(2000, 1, 1))
        remaining_months = st.number_input("Remaining Months", min_value=0, value=12)
        external_internal = st.selectbox("External/Internal", ["External", "Internal"])
        calibration_report_no = st.text_input("Calibration Report No")
        calibrator = st.text_input("Calibrator")
        pic = st.text_input("PIC")
        renewal_reminder = st.text_input("Renewal Reminder")

        updated_data = {
            "division": division, "description": description, "brand": brand, "tag": tag,
            "model_part_no": model_part_no,
            "serial_id_no": serial_id_no, "measurement_range": measurement_range,
            "tolerance_limit_external": tolerance_limit_external,
            "tolerance_limit_internal": tolerance_limit_internal, "in_use": in_use,
            "calibration_interval": calibration_interval,
            "last_calibration": last_calibration, "calibration_due": calibration_due,
            "remaining_months": remaining_months,
            "external_internal": external_internal, "calibration_report_no": calibration_report_no,
            "calibrator": calibrator,
            "pic": pic, "renewal_reminder": renewal_reminder
        }
        update_button = st.form_submit_button("Update Calibration")
        if update_button:
            update_calibration_data(update_id, updated_data)

def dashboard_page():
    st.title("Power BI Dashboard")
    power_bi_url = "YOUR_POWER_BI_EMBED_URL_HERE"  # Replace with your actual Power BI embed URL

    if power_bi_url:
        components.iframe(power_bi_url, height=600, scrolling=True)
    else:
        st.warning("Please provide a Power BI embed URL to display the dashboard.")

# === MAIN STREAMLIT APP ===
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to:", ["Calendar", "Calibration Management", "Dashboard"])

    calendar_id = 'boscalibration@gmail.com'  # Replace with your calendar ID
    timezone = 'Asia/Singapore'  # Replace with your desired timezone

    if page == "Calendar":
        display_calendar_page(calendar_id, timezone)
    elif page == "Calibration Management":
        calibration_management_page()
    elif page == "Dashboard":
        dashboard_page()

if __name__ == "__main__":
    main()
