# model_training_multiple_predictions_dag.py

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import mean_squared_error
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import pickle

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'model_training_multiple_predictions_dag',
    default_args=default_args,
    description='DAG for predicting multiple aspects of calibration tools',
    schedule_interval=timedelta(days=60),
    catchup=False,
)

# Function to fetch data
def fetch_data_from_db():
    conn = psycopg2.connect(
        host="localhost",
        database="calibration_db",
        user="root",
        password="d0a224cb89"
    )
    query = """
    SELECT t.Serial_Id_No, t.Measurement_Range, u.actual_interval_years, u.in_use,
           c.last_calibration_date, c.next_due_date, c.remaining_months, c.calibration_status
    FROM tools t
    JOIN calibration c ON t.Serial_Id_No = c.Serial_Id_No
    JOIN toolusage u ON t.Serial_Id_No = u.Serial_Id_No
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Preprocess the data
def preprocess_data(df):
    df['in_use'] = df['in_use'].astype(int)
    df.fillna(df.mean(), inplace=True)
    return df

# Train a model for remaining_months prediction
def train_remaining_months_model():
    df = fetch_data_from_db()
    df = preprocess_data(df)
    
    X = df[['actual_interval_years', 'in_use', 'Measurement_Range']]
    y = df['remaining_months']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Remaining Months Model - Mean Squared Error: {mse}')
    
    with open('/tmp/remaining_months_model.pkl', 'wb') as f:
        pickle.dump(model, f)

# Train a model for calibration_status prediction (classification task)
def train_calibration_status_model():
    df = fetch_data_from_db()
    df = preprocess_data(df)
    
    X = df[['actual_interval_years', 'in_use', 'Measurement_Range']]
    y = df['calibration_status'].apply(lambda x: 1 if x == 'completed' else 0)  # Binary classification (completed vs non-completed)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    accuracy = (y_pred == y_test).mean()
    print(f'Calibration Status Model - Accuracy: {accuracy}')
    
    with open('/tmp/calibration_status_model.pkl', 'wb') as f:
        pickle.dump(model, f)

# Function to update predictions back to DB
def update_predictions():
    with open('/tmp/remaining_months_model.pkl', 'rb') as f:
        remaining_months_model = pickle.load(f)
    
    with open('/tmp/calibration_status_model.pkl', 'rb') as f:
        calibration_status_model = pickle.load(f)

    df = fetch_data_from_db()
    df = preprocess_data(df)

    X = df[['actual_interval_years', 'in_use', 'Measurement_Range']]

    # Predict remaining months
    df['predicted_remaining_months'] = remaining_months_model.predict(X)

    # Predict calibration status (completed = 1, failed = 0)
    df['predicted_calibration_status'] = calibration_status_model.predict(X)

    conn = psycopg2.connect(
        host="localhost",
        database="calibration_db",
        user="root",
        password="d0a224cb89"
    )
    cursor = conn.cursor()

    for index, row in df.iterrows():
        update_query = f"""
        UPDATE calibration
        SET remaining_months = %s, calibration_status = %s
        WHERE Serial_Id_No = %s
        """
        cursor.execute(update_query, (row['predicted_remaining_months'], 
                                      'completed' if row['predicted_calibration_status'] == 1 else 'failed', 
                                      row['Serial_Id_No']))

    conn.commit()
    cursor.close()
    conn.close()

# Task 1: Train Remaining Months Model
train_remaining_months_task = PythonOperator(
    task_id='train_remaining_months_model_task',
    python_callable=train_remaining_months_model,
    dag=dag,
)

# Task 2: Train Calibration Status Model
train_calibration_status_task = PythonOperator(
    task_id='train_calibration_status_model_task',
    python_callable=train_calibration_status_model,
    dag=dag,
)

# Task 3: Update Predictions
update_predictions_task = PythonOperator(
    task_id='update_predictions_task',
    python_callable=update_predictions,
    dag=dag,
)

# Define task dependencies
train_remaining_months_task >> train_calibration_status_task >> update_predictions_task
