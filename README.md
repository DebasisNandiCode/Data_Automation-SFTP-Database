# Data_Automation-SFTP-Database

## Overview
This repository contains a Python script for automating data workflows involving:
1. **SFTP File Download:** Securely connecting to an SFTP server to download files.
2. **Data Preprocessing:** Cleaning and transforming data from CSV files, including handling numeric, currency, and datetime columns.
3. **Database Integration:** Uploading cleaned data to an SQL database.
4. **Logging and Notifications:** Comprehensive logging and email alerts for status updates and error reporting.

---

## Features
- **SFTP Automation**:
  - Securely connects to an SFTP server.
  - Downloads files based on specific criteria.
  - Archives downloaded files in a remote archive directory.

- **Data Cleaning**:
  - Handles invalid and missing values (`NULL`, `N/A`, etc.).
  - Processes numeric and currency columns for database compatibility.
  - Parses and standardizes datetime fields.

- **Database Upload**:
  - Uses SQLAlchemy for seamless database integration.
  - Supports MSSQL with ODBC Driver 17.

- **Error Handling & Notifications**:
  - Logs errors and success messages to a custom logging system.
  - Sends email alerts for failures or process completion.

---

## Prerequisites
1. Python 3.8+
2. Required Python packages:
   - `pandas`
   - `sqlalchemy`
   - `pysftp`
   - `smtplib`
3. Database with proper credentials for MSSQL connection.
4. SMTP setup for sending emails.

---

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/<username>/Data_Automation-SFTP-Database.git
