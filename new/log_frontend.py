import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO

# Function to connect to MySQL
def create_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="calibration_db",
            user="root",
            password="d0a224cb89"
        )
        return connection
    except Error as e:
        st.error(f"Error: {e}")
        return None

# Streamlit form for calibration logging
def log_calibration():
    st.title("Log Calibration Event")

    # Form fields
    machine_serial = st.text_input("Machine Serial Number")
    technician_name = st.text_input("Technician Name")
    calibration_notes = st.text_area("Calibration Notes")
    calibration_status = st.selectbox("Calibration Status", ["completed", "failed", "overdue"])

    if st.button("Submit Calibration"):
        if machine_serial and technician_name:
            # Insert calibration log into the database
            connection = create_connection()
            if connection:
                cursor = connection.cursor()
                # Insert into calibration_log table
                query = """INSERT INTO calibration_log (Serial_Id_No, calibration_date, technician_name, calibration_notes, calibration_status)
                           VALUES (%s, NOW(), %s, %s, %s)"""
                cursor.execute(query, (machine_serial, technician_name, calibration_notes, calibration_status))
                connection.commit()

                # Update calibration table for the next calibration date
                query_update = """UPDATE calibration
                                  SET last_calibration_date = NOW(),
                                      next_due_date = DATE_ADD(NOW(), INTERVAL (remaining_months * 30) DAY)
                                  WHERE Serial_Id_No = %s"""
                cursor.execute(query_update, (machine_serial,))
                connection.commit()

                st.success("Calibration event logged successfully!")
            else:
                st.error("Database connection failed.")
        else:
            st.error("Please fill in all required fields.")

def generate_pdf_report():
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    
    styles = getSampleStyleSheet()
    elements.append(Paragraph("Calibration Management Report", styles['Title']))
    
    # Add summary data (example)
    data = [
        ["Metric", "Value"],
        ["Total Calibrations", "185"],
        ["Completed Calibrations", "120"],
        ["Pending Calibrations", "45"],
        ["Overdue Calibrations", "15"],
        ["Failed Calibrations", "5"],
        ["Average Calibration Time", "60 minutes"],
    ]
    
    t = Table(data)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('TOPPADDING', (0, 1), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(t)
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

def generate_dashboard():
    st.title("Calibration Management Dashboard")

    if st.button("Refresh Dashboard"):
        st.experimental_rerun()

    # Add Generate PDF Report button
    if st.button("Generate PDF Report"):
        pdf = generate_pdf_report()
        st.download_button(
            label="Download PDF Report",
            data=pdf,
            file_name="calibration_report.pdf",
            mime="application/pdf"
        )

    # 1. Calibration Status Distribution (Pie Chart)
    st.subheader("1. Calibration Status Distribution")
    status_data = {'Status': ['Completed', 'Pending', 'Overdue', 'Failed'],
                   'Count': [120, 45, 15, 5]}
    fig1, ax1 = plt.subplots()
    ax1.pie(status_data['Count'], labels=status_data['Status'], autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')
    st.pyplot(fig1)

    # 2. Calibrations Due by Department (Bar Chart)
    st.subheader("2. Calibrations Due by Department")
    dept_data = pd.DataFrame({
        'Department': ['Engineering', 'Production', 'Quality Control', 'R&D', 'Maintenance'],
        'Calibrations Due': [25, 18, 30, 12, 15]
    })
    st.bar_chart(dept_data.set_index('Department'))

    # 3. Calibration Frequency by Tool Type (Horizontal Bar Chart)
    st.subheader("3. Calibration Frequency by Tool Type")
    tool_data = pd.DataFrame({
        'Tool Type': ['Multimeter', 'Oscilloscope', 'Pressure Gauge', 'Thermometer', 'Scale'],
        'Frequency (months)': [6, 12, 3, 9, 6]
    })
    fig3, ax3 = plt.subplots(figsize=(10, 5))
    sns.barplot(x='Frequency (months)', y='Tool Type', data=tool_data, orient='h')
    st.pyplot(fig3)

    # 4. Calibration Costs Over Time (Line Chart)
    st.subheader("4. Calibration Costs Over Time")
    cost_data = pd.DataFrame({
        'Month': pd.date_range(start='2023-01-01', periods=12, freq='M'),
        'Cost': [5000, 5200, 4800, 5100, 5300, 5600, 5400, 5200, 5500, 5700, 5900, 6000]
    })
    st.line_chart(cost_data.set_index('Month'))

    # 5. Tool Accuracy Trend (Line Chart with Multiple Lines)
    st.subheader("5. Tool Accuracy Trend")
    accuracy_data = pd.DataFrame({
        'Date': pd.date_range(start='2023-01-01', periods=10, freq='M'),
        'Tool A': [99, 98, 98, 97, 97, 96, 97, 98, 98, 99],
        'Tool B': [97, 97, 96, 96, 95, 95, 94, 95, 96, 96],
        'Tool C': [98, 98, 99, 99, 98, 98, 97, 97, 98, 98]
    })
    st.line_chart(accuracy_data.set_index('Date'))

    # 6. Calibration Time Distribution (Histogram)
    st.subheader("6. Calibration Time Distribution")
    time_data = np.random.normal(60, 15, 1000)  # Mean 60 minutes, std dev 15 minutes
    fig6, ax6 = plt.subplots()
    plt.hist(time_data, bins=20, edgecolor='black')
    plt.xlabel('Time (minutes)')
    plt.ylabel('Frequency')
    st.pyplot(fig6)

    # 7. Technician Performance (Radar Chart)
    st.subheader("7. Technician Performance")
    tech_data = pd.DataFrame({
        'Metric': ['Speed', 'Accuracy', 'Documentation', 'Tool Knowledge', 'Customer Satisfaction'],
        'Technician A': [4, 5, 3, 4, 5],
        'Technician B': [5, 4, 4, 3, 4],
        'Technician C': [3, 5, 5, 5, 3]
    })
    fig7, ax7 = plt.subplots(figsize=(6, 6), subplot_kw=dict(projection='polar'))
    theta = np.linspace(0, 2*np.pi, len(tech_data), endpoint=False)
    for tech in ['Technician A', 'Technician B', 'Technician C']:
        values = tech_data[tech].values
        values = np.concatenate((values, [values[0]]))
        ax7.plot(np.concatenate((theta, [theta[0]])), values)
        ax7.fill(np.concatenate((theta, [theta[0]])), values, alpha=0.1)
    ax7.set_xticks(theta)
    ax7.set_xticklabels(tech_data['Metric'])
    st.pyplot(fig7)

    # 8. Calibration Pass/Fail Rate by Tool Brand (Stacked Bar Chart)
    st.subheader("8. Calibration Pass/Fail Rate by Tool Brand")
    brand_data = pd.DataFrame({
        'Brand': ['Brand A', 'Brand B', 'Brand C', 'Brand D'],
        'Pass': [85, 90, 78, 88],
        'Fail': [15, 10, 22, 12]
    })
    fig8, ax8 = plt.subplots()
    brand_data.plot(x='Brand', y=['Pass', 'Fail'], kind='bar', stacked=True, ax=ax8)
    plt.ylabel('Percentage')
    st.pyplot(fig8)

    # 9. Calibration Backlog Trend (Area Chart)
    st.subheader("9. Calibration Backlog Trend")
    backlog_data = pd.DataFrame({
        'Date': pd.date_range(start='2023-01-01', periods=12, freq='M'),
        'Backlog': [50, 48, 52, 45, 40, 35, 30, 28, 25, 20, 18, 15]
    })
    st.area_chart(backlog_data.set_index('Date'))

    # 10. Tool Utilization vs Calibration Frequency (Scatter Plot)
    st.subheader("10. Tool Utilization vs Calibration Frequency")
    util_data = pd.DataFrame({
        'Utilization (%)': np.random.uniform(20, 100, 50),
        'Calibration Frequency (months)': np.random.uniform(1, 24, 50)
    })
    fig10, ax10 = plt.subplots()
    plt.scatter(util_data['Utilization (%)'], util_data['Calibration Frequency (months)'])
    plt.xlabel('Utilization (%)')
    plt.ylabel('Calibration Frequency (months)')
    st.pyplot(fig10)


# Main Streamlit App
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select a Page", ["Log Calibration", "Add/update calibration tools", "Dashboard"])

    if page == "Log Calibration":
        log_calibration()
    elif page == "Dashboard":
        generate_dashboard()

if __name__ == "__main__":
    main()
