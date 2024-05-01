import os
import boto3
import csv
import json
from datetime import datetime, timedelta

def write_to_csv(jobs, csv_filename):
    # Write the backup job information to a CSV file
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['Date', 'Resource Type', 'Resource Name', 'Resource ID', 'Completion Date', 'Backup Start Time', 'Backup End Time', 'State', 'Status Message', 'Message Category', 'Backup Size (GiB)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for job in jobs:
            backup_size_gib = job.get('BackupSizeInBytes', 0) / (1024 ** 3)  # Convert bytes to GiB
            writer.writerow({
                'Date': job.get('CreationDate', ''),
                'Completion Date': job.get('CompletionDate', ''),
                'Backup Start Time': job.get('StartBy', ''),
                'Backup End Time': job.get('CompletionDate', ''),
                'State': job.get('State', ''),
                'Message Category': job.get('MessageCategory', ''),  # Modify as per your requirements
                'Backup Size (GiB)': round(backup_size_gib, 2),  # Round to two decimal places
                'Resource ID': job.get('ResourceArn', ''),
                'Resource Type': job.get('ResourceType', ''),
                'Resource Name': job.get('ResourceName', ''),
                'Status Message': job.get('StatusMessage', '')
            })

def get_execution_month_year():
    # Get the current execution time and extract month and year
    now = datetime.utcnow()
    return now.year, now.month

def fetch_monthly_backup_jobs(backup_client, start_datetime, end_datetime):
    # Fetch backup jobs for each day in the specified date range
    jobs = []
    while start_datetime <= end_datetime:
        response = backup_client.list_backup_jobs(
            ByCreatedBefore=start_datetime + timedelta(days=1),  # Add 1 day to start_datetime to include the entire day
            ByCreatedAfter=start_datetime
        )
        jobs.extend(response.get('BackupJobs', []))
        start_datetime += timedelta(days=1)
    return jobs

def lambda_handler(event, context):
    try:
        # Retrieve environment variables
        s3_bucket_name = os.environ['S3_BUCKET_NAME']
        sns_topic_arn = os.environ['SNS_TOPIC_ARN']

        # Get the input month and year from the event, or use current execution time
        input_month = int(event.get('month', None))
        input_year = int(event.get('year', None))
        if input_month is None or input_year is None:
            input_year, input_month = get_execution_month_year()

        # Create a Boto3 AWS Backup client using the Lambda execution role's permissions
        backup_client = boto3.client('backup')
        s3_client = boto3.client('s3')
        sns_client = boto3.client('sns')

        # Calculate start and end dates for the specified month and year
        start_datetime = datetime(input_year, input_month, 1)
        end_datetime = start_datetime.replace(day=1, month=start_datetime.month % 12 + 1) - timedelta(days=1)

        # Fetch all backup jobs for the entire month
        jobs = fetch_monthly_backup_jobs(backup_client, start_datetime, end_datetime)

        # Generate a timestamp for the report
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')

        # Write the output to a CSV file with a timestamp
        csv_filename = f'/tmp/backup_jobs_{timestamp}.csv'
        write_to_csv(jobs, csv_filename)

        # Generate folder name with year-month-date
        folder_name = end_datetime.strftime('%Y-%m-%d')

        # Upload the CSV file to the specified folder in the S3 bucket
        s3_key = f'backup_report/{folder_name}/backup_jobs_{timestamp}.csv'
        s3_client.upload_file(csv_filename, s3_bucket_name, s3_key)

        # Send SNS notification with the timestamped S3 key
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='AWS Backup Job Report',
            Message=f'The AWS Backup job report for {end_datetime.strftime("%B %Y")} is available at: s3://{s3_bucket_name}/{s3_key}'
        )

        return {
            'statusCode': 200,
            'body': json.dumps('CSV file generated and SNS notification sent successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

if __name__ == "__main__":
    # For testing locally
    event = {}  # No input provided for month and year
    lambda_handler(event, {})
