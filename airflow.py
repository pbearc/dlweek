from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import requests
import mysql.connector
from datetime import datetime

# Power BI API Configuration
TENANT_ID = "."
CLIENT_ID = "."
CLIENT_SECRET = "."
WORKSPACE_ID = "."
DATASET_ID = "."

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": ".",
    "database": "calibration_db"
}

# Function to get Power BI access token
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json().get("access_token")

# Function to refresh Power BI dataset
def refresh_powerbi_dataset():
    access_token = get_access_token()
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.post(url, headers=headers)
    if response.status_code == 202:
        print("Power BI dataset refresh started successfully!")
    else:
        print(f"Failed to refresh dataset: {response.text}")

# Define the function to fetch due calibrations
def get_due_calibrations():
    today = datetime.today().date()
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT PIC, Description, Calibration_Due 
    FROM calibration_data 
    WHERE Calibration_Due = %s
    """
    
    cursor.execute(query, (today,))
    due_items = cursor.fetchall()
    cursor.close()
    conn.close()

    return due_items

# Define the function to send emails
def send_email_reminders():
    due_calibrations = get_due_calibrations()
    
    if not due_calibrations:
        print("No calibrations due today.")
        return
    
    for item in due_calibrations:
        email = f"{item['PIC']}@gmail.com"
        subject = "Calibration Due Reminder"
        body = f"Dear {item['PIC']},\n\nThe following calibration is due today:\n\n"
        body += f"Equipment: {item['Description']}\n"
        body += f"Due Date: {item['Calibration_Due']}\n\n"
        body += "Please ensure the calibration is completed on time.\n\nBest regards,\nCalibration Team"

        email_task = EmailOperator(
            task_id=f"send_email_{item['PIC']}",
            to=email,
            subject=subject,
            html_content=body,
            dag=dag
        )
        
        email_task.execute(context={})

# Airflow DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 1
}

dag = DAG(
    "calibration_and_powerbi_update",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # Runs daily at 1 AM
    catchup=False
)

# Task to send emails
email_task = PythonOperator(
    task_id="send_due_emails",
    python_callable=send_email_reminders,
    dag=dag
)

# Task to refresh Power BI dataset
refresh_task = PythonOperator(
    task_id="refresh_powerbi",
    python_callable=refresh_powerbi_dataset,
    dag=dag
)

# Define task order: Send emails first, then refresh Power BI
email_task >> refresh_task
