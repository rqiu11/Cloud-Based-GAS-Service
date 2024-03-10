import boto3
import json
from configparser import ConfigParser
from boto3.dynamodb.conditions import Key

# Load configuration
config = ConfigParser()
config.read('restore_config.ini')

# Initialize AWS services clients
sns = boto3.client('sns')
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')
s3 = boto3.client('s3')


dynamo = boto3.resource('dynamodb')
table_name = config['AWS']['table']
table = dynamo.Table(table_name)
def initiate_restore():
    while True:
        # Receive messages from SQS queue
        response = sqs.receive_message(QueueUrl=config['AWS']['SQS_QUEUE_URL'], MaxNumberOfMessages=10)
        if 'Messages' in response:  # Check if message is received
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            msg_body_str = response['Messages'][0]["Body"]

            msg_body = json.loads(msg_body_str)

            # Extract the 'Message' field, which is an escaped JSON string
            inner_message_str = msg_body['Message']

            # Second parse: Convert the 'Message' string to a JSON object
            data = json.loads(inner_message_str)
            
            # Get the user info that is upgrading to premium
            user_id = data["user_id"]
            print(user_id)
            response = table.query(
                IndexName='user_id_index',  
                KeyConditionExpression = Key('user_id').eq(user_id)
            )
            items = response['Items']
            print(items)
            # Retrieve files from Glacier
            # Reference: Restoring an archived object, https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects.html
            for item in items:
                # Check the status of the user when he submitted the job
                # Free users file gets archived and needs retrieval
                # Paid users file do not need a retrieval, as it is stored in bucket always
                if item['user_status'] == 'free_user':
                    archive_id = item["results_file_archive_id"]
                    file_id = item["s3_key_result_file"]
                    # Attempt expedited retrieval first
                    try:
                        response = glacier.initiate_job(
                            vaultName=config['AWS']['GLACIER_VAULT_NAME'],
                            jobParameters={
                                'Type': 'archive-retrieval',
                                'ArchiveId': archive_id,
                                'Tier': 'Expedited'
                            }
                        )
                    except glacier.exceptions.InsufficientCapacityException:
                        # Fall back to standard retrieval
                        response = glacier.initiate_job(
                            vaultName=config['AWS']['GLACIER_VAULT_NAME'],
                            jobParameters={
                                'Type': 'archive-retrieval',
                                'ArchiveId': archive_id,
                                'Tier': 'Standard'
                            }
                        )
                    # Send a notification to restore_init queue for thaw.py to check on the status of the retrieval
                    job_id = response["jobId"]
                    data = {
                        "file_id" : file_id,
                        "job_id": job_id,
                        "archive_id" : archive_id
                    }
                    topicARN = "arn:aws:sns:us-east-1:659248683008:rqiu_restore_init"
                    response = sns.publish(
                        TopicArn=topicARN,
                        Message=json.dumps({'default': json.dumps(data)}),
                        MessageStructure='json')
                    print(f"Initiated retrieval job for archive {archive_id} with response: {response}")


            
            # Delete the message from the queue
            sqs.delete_message(QueueUrl=config['AWS']['SQS_QUEUE_URL'], ReceiptHandle=receipt_handle)

if __name__ == '__main__':
    initiate_restore()
