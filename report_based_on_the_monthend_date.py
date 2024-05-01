import os
import boto3
import csv
import json
from datetime import datetime
import calendar

def extract_instance_id(resource_id):
    # Extract Instance ID from the Resource ID (assuming the Resource ID follows the format: arn:aws:ec2:region:account-id:instance/instance-id)
    parts = resource_id.split('/')
    return parts[-1] if len(parts) == 2 else ''

def write_to_csv(jobs, csv_filename):
    # Write the backup job information to a CSV file
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['Date', 'Resource Type', 'Resource Name', 'Resource ID', 'Instance ID', 'Completion Date', 'Backup Start Time', 'Backup End Time', 'State', 'Status Message', 'Message Category', 'Backup Size (GiB)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for job in jobs:
            backup_size_gib = job.get('BackupSizeInBytes', 0) / (1024 ** 3)  # Convert bytes to GiB
            resource_id = job.get('ResourceArn', '')
            instance_id = extract_instance_id(resource_id)

            writer.writerow({
                'Date': job.get('CreationDate', ''),
                'Completion Date': job.get('CompletionDate', ''),
                'Backup Start Time': job.get('StartBy', ''),
                'Backup End Time': job.get('CompletionDate', ''),
                'State': job.get('State', ''),
                'Message Category': job.get('MessageCategory', ''),  # Modify as per your requirements
                'Backup Size (GiB)': round(backup_size_gib, 2),  # Round to two decimal places
                'Resource ID': resource_id,
                'Instance ID': instance_id,
                'Resource Type': job.get('ResourceType', ''),
                'Resource Name': job.get('ResourceName', ''),
                'Status Message': job.get('StatusMessage', '')
            })

def get_last_day_of_month(year, month):
    return calendar.monthrange(year, month)[1]

def create_folder_in_s3(bucket_name, folder_path):
    s3_client = boto3.client('s3')
    # Check if the folder exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=folder_path)
    except Exception as e:
        # If the folder doesn't exist, create it
        if e.response['Error']['Code'] == '404':
            s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body='')
        else:
            raise

def lambda_handler(event, context):
    # Retrieve environment variables
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    # Extract the year and month from the user input
    year = int(event.get('year', datetime.utcnow().year))
    month = int(event.get('month', datetime.utcnow().month))

    # Calculate start and end dates for the specified month
    start_datetime = datetime(year, month, 1)
    end_datetime = datetime(year, month, get_last_day_of_month(year, month))

    # Create a Boto3 AWS Backup client using the Lambda execution role's permissions
    backup_client = boto3.client('backup')
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')

    # List all backup jobs within the specified date range
    jobs = []
    response = backup_client.list_backup_jobs(
        ByCreatedBefore=end_datetime,
        ByCreatedAfter=start_datetime
    )
    jobs.extend(response.get('BackupJobs', []))

    # Generate a timestamp for the report
    timestamp = start_datetime.strftime('%Y-%m')

    # Write the output to a CSV file with a timestamp
    csv_filename = f'/tmp/backup_jobs_{timestamp}.csv'
    write_to_csv(jobs, csv_filename)

    # Upload the CSV file to the specified S3 bucket with a timestamp
    s3_folder_path = f'backup_report/{year}-{month}-{get_last_day_of_month(year, month)}/'
    create_folder_in_s3(s3_bucket_name, s3_folder_path)
    s3_key = f'{s3_folder_path}backup_jobs_{timestamp}.csv'
    s3_client.upload_file(csv_filename, s3_bucket_name, s3_key)

    # Send SNS notification with the timestamped S3 key
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f'AWS Backup Job Report - {timestamp}',
        Message=f'The AWS Backup job report for {timestamp} is available at: s3://{s3_bucket_name}/{s3_key}'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('CSV file generated and SNS notification sent successfully!')
    }

if __name__ == "__main__":
    # For testing locally
    lambda_handler({}, {})
