
import os
import pandas as pd
from datetime import datetime, timedelta
import pysftp
import smtplib
import posixpath
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.message import MIMEMessage
from email import encoders
from sqlalchemy import create_engine
import urllib
from urllib.parse import quote_plus

from Logger import Logger # Logger class is created to maintain our program log 



# Initilize Logger
logger = Logger()

# Initialize Date time
today_date = datetime.now()
previous_date = today_date - timedelta(days=1)

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the target folder
raw_data = os.path.join(script_dir, 'Raw_Data')

# Get the previous date in the format mmddyyyy
previousDate = previous_date.strftime('%m%d%Y')

# Create a full path for the folder with previous's date
raw_data_folder = os.path.join(raw_data, previousDate)

# Create the folder if it doesn't exist
if not os.path.exists(raw_data_folder):
    os.makedirs(raw_data_folder)

print(f"Folder created at: {raw_data_folder}")


# Function to send log email
def send_email(subject, body, success=True):
    sender_email = "xxxxxxxx@xxxxxxxx.com"
    password = "*******"
    receiver_email = "xxxxxxxx@xxxxxxxx.com"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Attached email body
    msg.attach(MIMEText(body, 'plain'))

    # Setup email server
    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)

    except Exception as e:
        logger.log_to_database(0,"Error", "Email Failure", "Failed", "Email Failed", str(e), "Email Error")



# Define SFTP parameters
sftp_host = "xxxxxxx"
sftp_port = 22
sftp_username = "xxxxxxxx"
sftp_password = "xxxxxxx"
sftp_filepath = "/xxxx/xxxx/xxxx"
sftp_archivepath = "/xxxx/xxxx/xxxx/xxxx"

# Define SFTP connection options
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None # Disable host key checking (Use with caution for production)

# Connect to the SFTP Server
try:
    with pysftp.Connection(host=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        print("Connection Successfully Established...")

        # Download files from SFTP file path to AgentRawData
        try:
            # Change to the remote directory
            sftp.cwd(sftp_filepath)
            # List all files and directories in the remote directory
            items = sftp.listdir()

            # Download a particular file that starts with "My_Report" to the local AgentRawData folder
            for item in items:
                if item.startswith("My_Report"):  # Put your file name here
                    remote_item_path = posixpath.join(sftp_filepath, item)
                    local_file_path = os.path.join(raw_data_folder, item)

                    print(f"Found file: {item}. Downloading to: {local_file_path}")

                    # Download the file
                    sftp.get(remote_item_path, local_file_path)
                    print(f"Downloaded file: {item}")

                    # If needed, move the downloaded file to the SFTP archive folder
                    sftp_archive_file_path = posixpath.join(sftp_archivepath, item)
                    sftp.rename(remote_item_path, sftp_archive_file_path)
                    print(f"Moved SFTP file to archive: {sftp_archive_file_path}")

                    # Log success and exit loop after downloading
                    logger.log_to_database(0, "Info", "SFTP_File_Download", "Success", f"Downloaded file: {item}", "Success", "SFTP Download")
                    break  # Assuming only one file is to be downloaded
        
        except Exception as e:
            logger.log_to_database(0, "Error", "SFTP Error", "Failed", "Failed to download to local directory", str(e), "SFTP Error")
            send_email("Error! File Not found or download", f" not found, nor downloaded to local drive")
            print(f"An error occurred during  operation!: {str(e)}")    

except Exception as e:
    logger.log_to_database(0, "Error", "SFTP Connection Error", "Failed", "Failed to connect to SFTP server", str(e), "SFTP connection Error")
    send_email("Error! Unable to connect to SFTP server for Data", f"Error: {str(e)}")
    print(f"An error occurred while connecting to the SFTP server: {str(e)}")
    raise e



# Read CSV file
csv_files = [file for file in os.listdir(raw_data_folder) if file.endswith('.csv')]

if not csv_files:
    print(f"No CSV files found in folder {raw_data_folder}")
    logger.log_to_database(0, "Warning", "No CSV Files Found", "No Data", "No CSV files found for processing.", "No Data", "File Check")
    send_email("No CSV Files Found", "No CSV files found in the folder for processing.")
    exit()

else:
    csv_file_path = os.path.join(raw_data_folder, csv_files[0])
    try:
        # Read the CSV into a DataFrame
        df = pd.read_csv(csv_file_path)
        print(f"DataFrame shape: {df.shape}")

    except Exception as e:
        logger.log_to_database(0, "Error", "CSV Read Failure", "Failed", "Failed to read CSV file", str(e), "CSV Read Error")



# Check if the Datafrane is empty
if df.empty:
    logger.log_to_database(0,"Warning", "No Data", "No Data", "No Data in the downloaded file", "No Data", "Data checking")
    send_email("No Data in Folder", "The downloaded file is empty. No data insert into Database")
    print("No Data in the downloaded file. Email Sent.")

else:
    rename_col = {'Put your column name here with database column name for mapping Like "Employee Name : employee_name"'}

    # Rename DataFrame columns using mapping
    df.rename(columns=rename_col, inplace=True)

    numeric_column = ['Put your numeric column name here Like "Phone Numner : phone_number']

    decimal_column = ['Put your float column name here Like "Total Cost : total_cost"']

    # Function to clean numeric columns
    def clean_numeric_columns(df, columns):
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Function to clean decimal (currency) columns
    def clean_currency_columns(df, columns):
        for col in columns:
            if col in df.columns:
                # Replace NULL, N/A, and other invalid entries with NaN
                df[col] = df[col].replace(['NULL', 'N/A', '#N/A', '#DIV/0!', '', ' ', '#NULL!', '#NUM!', '#REF!', '#VALUE!', '#NAME?'], pd.NA)
                
                # Remove '$', convert to numeric, and replace NaN with 0.0
                df[col] = df[col].replace('[\$,]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)


    # Clean numeric and decimal columns
    clean_numeric_columns(df, numeric_column)
    clean_currency_columns(df, decimal_column)


    # Define the columns that contain datetime data
    datetime_columns = ['Put your date time column name here']

    for col in datetime_columns:
        if col in df.columns:
            # Replaces 'N/A', empty strings (''), and spaces (' ') with pd.NaT. This standardizes the missing value format for easier processing.
            df[col] = df[col].replace(['N/A', '', ' '], pd.NaT)

            # Create a parsed column for each datetime field
            parsed_col = col + "_Parsed"

            # The regular expression .str.replace(r'\s(EDT|EST)$', '', regex=True) removes the timezone abbreviations from the end of the datetime strings.
            df[parsed_col] = df[col].str.replace(r'\s(EDT|EST)$', '', regex=True).str.strip()
            df[parsed_col] = pd.to_datetime(df[parsed_col], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')

            # Fill NaT values with the default placeholder date "1900/01/01 00:00:00"
            df[parsed_col] = df[parsed_col].fillna(pd.Timestamp("1900-01-01 00:00:00"))


    # Define database connection
    try:
        username = 'xxxxxxxx'
        password = '********'
        password_encoded = urllib.parse.quote(password)
        server = 'xx.xx.xx.xx'
        database = 'xxxxxxxx'

        engine = create_engine(f'mssql+pyodbc://{username}:{password_encoded}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
        
        try:
            df.to_sql('Put your database table name here', con=engine, if_exists='append', index=False, chunksize=1000)

            logger.log_to_database(0,"Info", "Data Insert success", "Success", "data inserted successfully into DB", "Success", "Data Insertion")
            send_email("Data", "Data inserted successfully into Database.")
            print("Data inserted successfully into database.")

        except Exception as e:
            logger.log_to_database(0,"Error", "Data Insert Error", "Failed", "Error in data insertion", str(e), "Data Insertion Error")
            send_email("Error! Data", f"Error! - Data {str(e)}.")
            print("Error! Data Insertion")
        
    except Exception as e:
        send_email("Failed! Data Processing", f"Error! {str(e)}")
        raise e
