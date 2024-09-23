import boto3
import csv
from io import StringIO
from datetime import datetime
import redshift_connector
import logging


# Create SNS client
sns = boto3.client('sns')
sns_topic_arn = 'arn:aws:sns:us-east-1:296062584182:LambdaFailureNotifications'  

def send_failure_notification(error_message):
    subject = "Lambda Function Failure Notification"
    message = f"Error: {error_message}"
    sns.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject=subject
    )

def lambda_handler(event, context):
    # S3 client
    s3 = boto3.client('s3')
    
    # S3 bucket details
    bucket_name = 'new-redshift-kiwi'
    folder_prefix = 'project/'  # Folder where CSV files are stored
    archived_folder = 'archives/'  # Folder where archived files are moved
    inserted_folder = 'inserts/'  # Folder for saving inserted records CSV
    updated_folder = 'updates/'    # Folder for saving updated records CSV
    
    # Redshift Serverless connection details
    redshift_host = 'default-workgroup.296062584182.us-east-1.redshift-serverless.amazonaws.com'
    redshift_db = 'dev'
    redshift_user = 'admin'
    redshift_password = 'Doma_944'
    redshift_port = 5439
    
    # Table names
    stage_table = 'st_employees'
    production_table = 'pr_employees'
    audit_log_table = 'audit_log'
    
    start_time = datetime.now()  # Start time of the function execution
    timestamp = start_time.strftime('%Y%m%d%H%M%S')  # Timestamp for file names

    conn = None
    cur = None

    try:
        # List all files in the specified folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        if 'Contents' not in response:
            print("No files found in the specified folder.")
            return {
                'statusCode': 200,
                'body': "No CSV files found."
            }

        # Connect to Redshift
        conn = redshift_connector.connect(
            host=redshift_host,
            database=redshift_db,
            user=redshift_user,
            password=redshift_password,
            port=redshift_port
        )
        cur = conn.cursor()

        # Process files in the project folder
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.csv'):
                print(f"Processing file: {file_key}")

                # Initialize counts for this file
                total_inserted_count = 0
                total_updated_count = 0

                # Prepare StringIO objects to save the CSV contents for this file
                inserted_csv = StringIO()
                updated_csv = StringIO()

                # Prepare CSV writers
                inserted_writer = csv.writer(inserted_csv)
                updated_writer = csv.writer(updated_csv)

                # Write CSV headers
                csv_headers = ['Employee_ID', 'Employee_Name', 'Role', 'Shift_Type', 'Enter_Date', 'Salary']
                inserted_writer.writerow(csv_headers)
                updated_writer.writerow(csv_headers)

                # Get the CSV file from S3
                csv_response = s3.get_object(Bucket=bucket_name, Key=file_key)
                file_content = csv_response['Body'].read().decode('utf-8')
                
                # Use the csv module to read the CSV content
                csv_file = StringIO(file_content)
                csv_reader = csv.reader(csv_file)
                
                # Skip the header row (if the CSV contains headers)
                header = next(csv_reader, None)
                
                # Insert CSV data into the staging table
                print("Inserting data into Redshift staging table...")
                for row in csv_reader:
                    # Convert Enter_Date to integer
                    enter_date = int(row[4])
                    cur.execute(f"""
                        INSERT INTO {stage_table} (Employee_ID, Employee_Name, Role, Shift_Type, Enter_Date, Salary)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """, (row[0], row[1], row[2], row[3], enter_date, row[5]))
                conn.commit()

                # Process and insert new records into production table
                cur.execute(f"""
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary
                    FROM {stage_table} s
                    LEFT JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE p.Employee_ID IS NULL;
                """)
                inserted_rows = cur.fetchall()

                print("Inserting new records into production table...")
                cur.execute(f"""
                    INSERT INTO {production_table} (Employee_ID, Employee_Name, Role, Shift_Type, Enter_Date, Salary, aid)
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary, 
                    (SELECT COALESCE(MAX(aid), 0) + 1 FROM {audit_log_table})
                    FROM {stage_table} s
                    LEFT JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE p.Employee_ID IS NULL;
                """)
                conn.commit()

                # Write the inserted rows to the CSV
                for row in inserted_rows:
                    inserted_writer.writerow(row)
                total_inserted_count += len(inserted_rows)

                # Process and update records in production table
                cur.execute(f"""
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary
                    FROM {stage_table} s
                    JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE s.Employee_Name != p.Employee_Name 
                       OR s.Role != p.Role 
                       OR s.Shift_Type != p.Shift_Type 
                       OR s.Enter_Date != p.Enter_Date 
                       OR s.Salary != p.Salary;
                """)
                updated_rows = cur.fetchall()

                print("Updating records in production table...")
                cur.execute(f"""
                    UPDATE {production_table} p
                    SET Employee_Name = s.Employee_Name,
                        Role = s.Role,
                        Shift_Type = s.Shift_Type,
                        Enter_Date = s.Enter_Date,
                        Salary = s.Salary,
                        aid = (SELECT COALESCE(MAX(aid), 0) + 1 FROM {audit_log_table})
                    FROM {stage_table} s
                    WHERE p.Employee_ID = s.Employee_ID
                    AND (s.Employee_Name != p.Employee_Name 
                         OR s.Role != p.Role 
                         OR s.Shift_Type != p.Shift_Type 
                         OR s.Enter_Date != p.Enter_Date 
                         OR s.Salary != p.Salary);
                """)
                conn.commit()

                # Write the updated rows to the CSV
                for row in updated_rows:
                    updated_writer.writerow(row)
                total_updated_count += len(updated_rows)

                # Move the processed file to the archives folder
                archive_key = file_key.replace(folder_prefix, archived_folder, 1)
                print(f"Moving file to archives: {archive_key}")
                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=archive_key)
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                print(f"Deleted original file: {file_key}")

                # Upload the inserted records CSV to S3
                inserted_file_key = f"{inserted_folder}inserted_{timestamp}_{file_key.split('/')[-1]}"
                s3.put_object(Bucket=bucket_name, Key=inserted_file_key, Body=inserted_csv.getvalue())
                print(f"Inserted records saved to: {inserted_file_key}")

                # Upload the updated records CSV to S3
                updated_file_key = f"{updated_folder}updated_{timestamp}_{file_key.split('/')[-1]}"
                s3.put_object(Bucket=bucket_name, Key=updated_file_key, Body=updated_csv.getvalue())
                print(f"Updated records saved to: {updated_file_key}")

                # Truncate the staging table for future insertions
                print("Truncating the staging table...")
                cur.execute(f"TRUNCATE TABLE {stage_table};")
                conn.commit()

                # Insert a new record into the audit log with the counts for this file
                cur.execute(f"""
                    INSERT INTO {audit_log_table} (updated, inserted, start_date, end_date)
                    VALUES (%s, %s, %s, %s);
                """, (total_updated_count, total_inserted_count, start_time, datetime.now()))
                conn.commit()

        # Close the connection
        cur.close()
        conn.close()

        print("CSV data processed and Redshift updated.")
        
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error processing the file or Redshift operations: {error_message}")
        if conn:
            conn.rollback()
        send_failure_notification(error_message)  # Send notification on failure
        return {
            'statusCode': 500,
            'body': f"Error: {error_message}"
        }
    
    return {
        'statusCode': 200,
        'body': "CSV files processed and Redshift updated."
    }
