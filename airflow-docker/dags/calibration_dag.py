from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from kafka import KafkaProducer
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),  # Replace with your desired start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'calibration_alerts_etl',
    default_args=default_args,
    description='ETL pipeline for calibration alerts',
    schedule_interval='@daily',  # Adjust according to your needs
)

def send_alert(message):
    # Set up the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # Replace with your Kafka server's address
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
    )

    # Send the alert message to the 'calibration_alerts' topic
    producer.send('calibration_alerts', value=message)

    # Ensure message is sent
    producer.flush()

    print(f"Sent alert: {message}")
    producer.close()


# Function for the ETL process
def etl_process():
    # Connect to MySQL
    engine = create_engine("mysql+pymysql://root:d0a224cb89@localhost/calibration_db")

    # 1. Extract data
    try:
        query = """
        SELECT 
            t.Serial_Id_No,
            t.Division,
            t.Description,
            t.Brand,
            t.Tag,
            t.Model_Part_No,
            t.Measurement_Range,
            t.Tolerance_Limit_External,
            t.Tolerance_Limit_Internal,
            c.last_calibration_date,
            c.next_due_date,
            c.remaining_months,
            c.external_internal,
            c.report_number,
            c.calibrator,
            tu.in_use,
            tu.actual_interval_years
        FROM tools t
        JOIN calibration c ON t.Serial_Id_No = c.Serial_Id_No
        JOIN toolusage tu ON t.Serial_Id_No = tu.Serial_Id_No
        """
        df = pd.read_sql(query, engine)
        print("✅ Data extracted successfully from MySQL.")
    except Exception as e:
        print(f"❌ Error during data extraction: {e}")
        exit()

    # 2. Transform data
    try:
        # Convert date columns to datetime objects
        df['last_calibration_date'] = pd.to_datetime(df['last_calibration_date'], errors='coerce')
        df['next_due_date'] = pd.to_datetime(df['next_due_date'], errors='coerce')

        # Handle potential NaT values after conversion
        df = df.dropna(subset=['last_calibration_date', 'next_due_date'])

        # Calculate days until due
        now = pd.Timestamp.now()
        df['days_until_due'] = (df['next_due_date'] - now).dt.days
        df['days_until_due'] = df['days_until_due'].clip(lower=0)  # Ensure no negative values

        def calculate_risk_score(days_until_due):
            if days_until_due <= 30:
                return 'High'
            elif days_until_due <= 90:
                return 'Medium'
            else:
                return 'Low'

        df['risk_score'] = df['days_until_due'].apply(calculate_risk_score)

        print("✅ Data transformed successfully.")
    except Exception as e:
        print(f"❌ Error during data transformation: {e}")
        exit()

    # 3. Load transformed data into a new table
    try:
        df.to_sql('transformed_calibration_data', engine, if_exists='replace', index=False)
        print("✅ Data loaded successfully to transformed_calibration_data table.")
    except Exception as e:
        print(f"❌ Error during data loading: {e}")
        exit()

    print("✅ ETL pipeline completed successfully!")

    # Send alert after the ETL process
    message = "Calibration due alert: Check tools with high risk!"
    send_alert(message)

# Define the ETL task
etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl_process,
    dag=dag,
)

def post_process():
    print("🔄 Post-processing tasks are running!")

refresh_dashboard = PythonOperator(
    task_id='refresh_dashboard',
    python_callable=post_process,
    dag=dag,
)

# Set the task dependencies
etl_task >> refresh_dashboard  

