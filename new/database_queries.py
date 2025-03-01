from sqlalchemy import create_engine
import pandas as pd

def query_due_calibrations():
    # Database connection
    engine = create_engine('mysql+pymysql://root:d0a224cb89@localhost/calibration_db')
    
    query = "SELECT * FROM calibrations WHERE due_date <= NOW()"
    due_calibrations = pd.read_sql(query, engine)
    return due_calibrations
