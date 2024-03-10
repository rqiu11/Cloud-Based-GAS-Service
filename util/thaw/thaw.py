import boto3
import json
import time
import os
from configparser import ConfigParser
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Load configuration
config = ConfigParser()
config.read('thaw_config.ini')

# Initialize AWS services clients
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')
s3 = boto3.client('s3')
# Initialize a Glacier client
glacier = boto3.client('glacier', region_name='us-east-1')

# Specify your Glacier vault name and the job ID
vault_name = config['AWS']['VAULT_NAME']
bucket = config['AWS']['S3_BUCKET_NAME']
# Constantly listening to the restore init queue
# Message contains the archive ID, file name, and Job ID 
# If a file is retrieved from the Glacier, upload it back to S3 bucket. 
while True:
# Call describe_job to check the job's status
    response = sqs.receive_message(QueueUrl=config['AWS']['SQS_QUEUE_URL'], MaxNumberOfMessages=10)
    if 'Messages' in response:  # Check if message is received
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        msg_body_str = response['Messages'][0]["Body"]

        msg_body = json.loads(msg_body_str)

        # Extract the 'Message' field, which is an escaped JSON string
        inner_message_str = msg_body['Message']

        # Second parse: Convert the 'Message' string to a JSON object
        data = json.loads(inner_message_str)
        job_id = data["job_id"]
        file_id = data["file_id"]
        archive_id = data["archive_id"]
        file_name = os.path.basename(file_id)
        object_name = 'rqiu/' + file_name

        response = glacier.describe_job(vaultName=vault_name, jobId=job_id)


        # Print the job's status
        status_code = response['StatusCode']

        while status_code == "InProgress":
            print("Retrieval job is still in progress.")
            # Check status of the retrieval every minute
            # Reference: Using the AWS SDKs with Amazon S3 Glacier, https://docs.aws.amazon.com/amazonglacier/latest/dev/using-aws-sdk.html
            response = glacier.describe_job(vaultName=vault_name, jobId=job_id)
            status_code = response['StatusCode']
            time.sleep(60)

        # Successfully retrieved the file. We can delete the message from queue.
            
        if status_code == 'Succeeded':
            print("Retrieval job completed successfully.")
            sqs.delete_message(QueueUrl=config['AWS']['SQS_QUEUE_URL'], ReceiptHandle=receipt_handle)
            # Download the file from glacier
            # Reference:Get Amazon S3 Glacier job output using an AWS SDK, https://docs.aws.amazon.com/amazonglacier/latest/dev/example_glacier_GetJobOutput_section.html
            response = glacier.get_job_output(vaultName=vault_name, jobId=job_id)

            # Write the output to a file
            with open(file_name, 'wb') as file:
                file.write(response['body'].read())
            try:
                # Upload to S3
                s3.upload_file(file_name, bucket, object_name)
                print("Successfully uploaded to S3")
                # Delete the archive and local file
                response = glacier.delete_archive(vaultName=vault_name, archiveId=archive_id)
                print('Archive deleted successfully.')
                if os.path.exists(file_name):
                        os.remove(file_name)
            except ClientError as e:
                print(f"Upload error {e}")
        else:
            print("Retrieval job failed or is in an unexpected state.")
        
        
