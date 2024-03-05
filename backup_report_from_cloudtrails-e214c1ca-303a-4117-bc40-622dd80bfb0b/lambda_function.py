import os
import boto3
import json
import csv
from datetime import datetime, timedelta

def bytes_to_gib(bytes_size):
    gib_size = bytes_size / (1024 ** 3)  # Convert bytes to gibibytes
    return round(gib_size, 2)

def lambda_handler(event, context):
    cloudtrail_client = boto3.client('cloudtrail')
    sns_client = boto3.client('sns')
    s3_client = boto3.client('s3')

    # Calculate default start and end times for the last 24 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    # Override with input event times if provided
    if 'startTime' in event:
        start_time = event['startTime']
    if 'endTime' in event:
        end_time = event['endTime']

    response = cloudtrail_client.lookup_events(
        LookupAttributes=[
            {'AttributeKey': 'EventName', 'AttributeValue': 'BackupJobCompleted'}
        ],
        StartTime=start_time,
        EndTime=end_time
    )

    events = response.get('Events', [])
    csv_data = [
        ['EVENT TIME', 'STATE', 'PERCENT DONE', 'RESOURCE ID', 'BACKUP SIZE (GiB)', 'RESOURCE TYPE']
    ]

    for event in events:
        cloud_trail_event = json.loads(event.get('CloudTrailEvent', '{}'))
        service_event_details = cloud_trail_event.get('serviceEventDetails', {})
        
        event_time = event.get('EventTime', '')
        state = service_event_details.get('state', '')
        percent_done = service_event_details.get('percentDone', '')
        resource_arn = service_event_details.get('resourceArn', '')
        # Extracting resource ID from the ARN
        resource_id = resource_arn.split(':')[-1]
        backup_size_bytes = service_event_details.get('backupSizeInBytes', 0)
        backup_size_gib = bytes_to_gib(backup_size_bytes)
        resource_type = service_event_details.get('resourceType', '')

        csv_data.append([event_time, state, percent_done, resource_id, backup_size_gib, resource_type])

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'/tmp/prj-env-month-report_{timestamp}.csv'

    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(csv_data)

    # Upload the CSV file to S3
    s3_bucket = os.environ['S3_BUCKET_NAME']
    s3_key = f'aws-backup-report_{timestamp}.csv'
    s3_object_location = f's3://{s3_bucket}/{s3_key}'
    s3_client.upload_file(csv_filename, s3_bucket, s3_key)

    # Send SNS notification with the generated CSV file as an attachment
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    sns_subject = 'Backup Job Report'
    sns_message = f'Please find the attached CSV file for backup job report.\nS3 Bucket: {s3_bucket}\nObject Location: {s3_object_location}'

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=sns_subject,
        Message=json.dumps({'default': sns_message}),
        MessageStructure='json',
        MessageAttributes={
            'Attachment': {
                'DataType': 'String',
                'StringValue': s3_object_location
            }
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('CSV file generated and SNS notification sent successfully!')
    }
