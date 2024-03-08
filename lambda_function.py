
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

def lambda_handler(event, context):
    # Retrieve environment variables
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']

    # Create a Boto3 AWS Backup client using the Lambda execution role's permissions
    backup_client = boto3.client('backup')
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')

    # Calculate start and end dates for the last 1 month
    end_datetime = datetime.utcnow()
    start_datetime = end_datetime - timedelta(days=30)

    # List all backup jobs within the specified date range, including failed and canceled jobs
    jobs = []
    while start_datetime <= end_datetime:
        response = backup_client.list_backup_jobs(
            ByCreatedBefore=end_datetime,
            ByCreatedAfter=start_datetime
        )
        jobs.extend(response.get('BackupJobs', []))
        end_datetime -= timedelta(days=1)

    # Generate a timestamp for the report
    timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')

    # Write the output to a CSV file with a timestamp
    csv_filename = f'/tmp/backup_jobs_{timestamp}.csv'
    write_to_csv(jobs, csv_filename)

    # Upload the CSV file to the specified S3 bucket with a timestamp
    s3_key = f'backup_report/backup_jobs_{timestamp}.csv'
    s3_client.upload_file(csv_filename, s3_bucket_name, s3_key)

    # Send SNS notification with the timestamped S3 key
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject='AWS Backup Job Report',
        Message=f'The AWS Backup job report for the last 1 month is available at: s3://{s3_bucket_name}/{s3_key}'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('CSV file generated and SNS notification sent successfully!')
    }

if __name__ == "__main__":
    # For testing locally
    lambda_handler({}, {})






