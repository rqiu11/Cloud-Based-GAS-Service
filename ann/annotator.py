import boto3
import subprocess
import os
import json
import threading
import configparser

# Configure AWS SQS (ensure AWS credentials are set up)
sqs = boto3.client('sqs')
s3_client = boto3.client('s3')


config = configparser.ConfigParser()
config.read('ann_config.ini')
queue_url = config['AWS']['queue_url']
S3_BUCKET = config['AWS']['input_bucket']

dynamo = boto3.resource('dynamodb')
table_name = config['Table']['table']
table = dynamo.Table(table_name)

def download_file_from_s3(bucket_name, key, download_path):
    """
    Download a file from S3 to the specified local path.
    """
    s3_client.download_file(Bucket=bucket_name, Key=key, Filename=download_path)
    return

def run_annotation(input_file_path,job_id, email, user_status):
    """
    Run the AnnTools annotation process on the input file.
    Generate a unique job ID for this annotation job.
    """
    print("Running " + job_id)
    # Construct the AnnTools command
    command = ["python3", 'run.py', input_file_path, job_id, email, user_status]

    # Run the AnnTools command
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return

# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue using long polling
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20  # Enable long polling, up to 20 seconds
    )

    if 'Messages' in response:  # Check if message is received
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        msg_body_str = response['Messages'][0]["Body"]

        msg_body = json.loads(msg_body_str)

        # Extract the 'Message' field, which is an escaped JSON string
        inner_message_str = msg_body['Message']

        # Second parse: Convert the 'Message' string to a JSON object
        data = json.loads(inner_message_str)
        
        # Here we are modifying our run.py to take two more argument: email and status
        # Email is used by run.py to send out notification when job is finished
        # Status is used by run.py to send out notification to the archive queue if user does not pay
        # We could have also done table query in run.py tp retrieve the user tier 
        # but this would be less communication and cost

        key = data["s3_key_input_file"]
        job_id = data["job_id"]
        email = data["email"]
        user_status = data['user_status']

        # Prepare the local download path

        download_path = './' + os.path.basename(key)

        bucket = data["s3_inputs_bucket"]
        # Download the file from S3
        download_file_from_s3(bucket, key, download_path)
        
        # Run the annotation job
        thread = threading.Thread(target=run_annotation, args=(download_path,job_id,email, user_status))
        thread.start()
        try:
        # Perform the conditional update
            response = table.update_item(
            Key={
                'job_id': job_id
                },
            UpdateExpression='SET job_status = :new_status',
            ConditionExpression='job_status = :current_status',
            ExpressionAttributeValues={
                            ':new_status': 'RUNNING',
                            ':current_status': 'PENDING'
                        },
            ReturnValues="UPDATED_NEW"
            )
            print("Update succeeded:", job_id)
        except dynamo.meta.client.exceptions.ConditionalCheckFailedException:
            # Handle the case where the condition is not met
            print("Condition not met, item not updated.")
        except Exception as e:
            # Handle other possible exceptions
            print(e)

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"Message for job {job_id} deleted from the queue.")
