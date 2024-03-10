import boto3
import os
import json
from botocore.exceptions import ClientError
from configparser import ConfigParser

config = ConfigParser()
config.read('archive_config.ini')
s3_client = boto3.client('s3')
# Initialize AWS services clients
glacier = boto3.client('glacier')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
result_bucket = config['AWS']['result_bucket']
queue_url = config['AWS']['SQS_QUEUE_URL']
table = dynamodb.Table(config['AWS']['table'])

def archive_to_glacier():
    # Poll SQS for messages
    while True:
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
        if 'Messages' in response:
            messages = response['Messages']
            print('Message received')
            for message in messages:
                try:
                    receipt_handle = message['ReceiptHandle']
                    msg_body_str = message['Body']
                    msg_body = json.loads(msg_body_str)

                    # Extract the 'Message' field, which is an escaped JSON string
                    inner_message_str = msg_body['Message']

                    # Second parse: Convert the 'Message' string to a JSON object
                    data = json.loads(inner_message_str)
                    job_id = data['job_id']
                    key = data['file']
                    download_path = os.path.basename(key)
                    s3_client.download_file(Bucket=result_bucket, Key=key, Filename=download_path)
                    # Move file from S3 to Glacier
                    archive_id = move_to_glacier(download_path)
                    
                    # Update DynamoDB with Glacier archive ID
                    if archive_id:
                        update_dynamodb(job_id, archive_id)
                        
                    # Delete message from SQS queue
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                    print(download_path)
                    s3_client.delete_object(Bucket=result_bucket, Key=key)
                    if os.path.exists(download_path):
                        os.remove(download_path)
                except Exception as e:
                    print(f"Error processing message: {e}")
# Move the downloaded file to glacier
#Reference: Upload an archive to an Amazon S3 Glacier vault using an AWS SDK, https://docs.aws.amazon.com/amazonglacier/latest/dev/example_glacier_UploadArchive_section.html
def move_to_glacier(filename):
    try:
        with open(filename, 'rb') as file:
        # Upload file to Glacier vault
            response = glacier.upload_archive(vaultName='mpcs-cc', 
                                          archiveDescription='Your file description', 
                                          body=file)
        
        # The response includes the archive ID, which you should save
            archive_id = response['archiveId']
            print(f"File uploaded to Glacier. Archive ID: {archive_id}")

        # Return the archive ID for further use
            return archive_id
    except ClientError as e:
        print(f"Error moving file to Glacier: {e}")
        return None

def update_dynamodb(job_id, archive_id):
    try:
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="set results_file_archive_id = :r",
            ExpressionAttributeValues={':r': archive_id},
            ReturnValues="UPDATED_NEW"
        )
        print(f"DynamoDB update response: {response}")
    except ClientError as e:
        print(f"Error updating DynamoDB: {e}")

if __name__ == '__main__':
    archive_to_glacier()
