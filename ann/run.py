import sys
import time
import driver
import boto3
import os
import logging
import shutil
import json
import configparser
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

s3_client = boto3.client('s3')

config = configparser.ConfigParser()
config.read('ann_config.ini')


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_file_to_s3(file_path, bucket_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    object_name = "/rqiu/" + os.path.basename(file_path)
    

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
    except ClientError as e:
            logging.error(e)
            return False
    return True
    

def clean_up_local_file(file_path):
    """Delete a local file

    :param file_path: Path to the file to be deleted
    """
    os.remove(file_path)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file_name = sys.argv[1]
        job_id = sys.argv[2]
        # Added two more arguments for further operation
        email = sys.argv[3]
        user_status = sys.argv[4]
        print("run.py")
        print(config['AWS']['result_bucket'])
        with Timer():
            driver.run(input_file_name, 'vcf')

            bucket_name = config['AWS']['result_bucket']
            print(config['AWS']['result_bucket'])
            
            s3_folder = 'rqiu'
            object_name = os.path.basename(input_file_name)
            object_name = "rqiu/" + object_name



            input2 = input_file_name + '.count.log'
            object_name2 = os.path.basename(input2)
            object_name2 = "rqiu/" + object_name2
            upload_file(input2,bucket_name,object_name2)


            input3 = input_file_name[:-4] + ".annot.vcf"
            object_name3 = os.path.basename(input3)
            object_name3 = "rqiu/" + object_name3
            upload_file(input3,bucket_name,object_name3)

            input2 = "./" + input2
            input3 = "./" + input3
            input_file_name = "./" + input_file_name
            if os.path.exists(input2):
                os.remove(input2)
            if os.path.exists(input3):
                os.remove(input3)
            if os.path.exists(input_file_name):
                os.remove(input_file_name)



            dynamo = boto3.resource('dynamodb', region_name='us-east-1')

            dynamo = boto3.resource('dynamodb')
            table_name = config['Table']['table']
            table = dynamo.Table(table_name)
            response1 = table.update_item(
                Key={
                    'job_id': job_id
                    },
                    UpdateExpression='SET #attrName = :val',
                    ExpressionAttributeNames={
                        '#attrName': 'job_status'
                    },
                    ExpressionAttributeValues={
                        ':val': 'COMPLETED'
                    },
                    ReturnValues="UPDATED_NEW"
                )
            response2 = table.update_item(
                Key={
                    'job_id': job_id
                    },
                    UpdateExpression='SET s3_results_bucket = :val',
                    
                    ExpressionAttributeValues={
                        ':val': bucket_name
                    },
                    ReturnValues="UPDATED_NEW"
                )
            response3 = table.update_item(
                Key={
                    'job_id': job_id
                    },
                    UpdateExpression='SET s3_key_result_file = :val',
                    
                    ExpressionAttributeValues={
                        ':val': object_name3
                    },
                    ReturnValues="UPDATED_NEW"
                )
            response4 = table.update_item(
                Key={
                    'job_id': job_id
                    },
                    UpdateExpression='SET s3_key_log_file = :val',
                    
                    ExpressionAttributeValues={
                        ':val': object_name2
                    },
                    ReturnValues="UPDATED_NEW"
                )
            
            current_utc_datetime = datetime.utcnow()

            # Convert to epoch time in seconds
            epoch = int(current_utc_datetime.timestamp())
            response4 = table.update_item(
                Key={
                    'job_id': job_id
                    },
                    UpdateExpression='SET complete_time = :val',
                    
                    ExpressionAttributeValues={
                        ':val': epoch
                    },
                    ReturnValues="UPDATED_NEW"
                )
            # The result queue is handled by the lambda function to send out email
            # Pass the email and the message so lambda knows the address
            # Publish notification to result topic sns
            result_sns_client = boto3.client("sns")
            # Your SNS topic ARN (Amazon Resource Name)
            data_completion = {
                        "jobId": job_id,
                        "status": "completed",
                        "message": "Your job has completed successfully.",
                        "email": email
                    }
            topicARN = "arn:aws:sns:us-east-1:659248683008:rqiu_job_results"
            response = result_sns_client.publish(
                TopicArn=topicARN,
                Message=json.dumps({'default': json.dumps(data_completion)}),
                MessageStructure='json')
            
            # This code sends notification to archive queue, which is handled by archive.py
            # The queue was set a 5 minuted delay. So archive.py would only receive the message
            # after 5 minutes the queue receives it from the completion time.
            # Publish to the archive topic immediately after completed
            if (user_status =='free_user'):
                archive_sns_client = boto3.client("sns")
                data_glacier = {
                            "job_id": job_id,
                            "status": "completed",
                            "file": object_name3,
                            "email": email
                        }
                topicARN = "arn:aws:sns:us-east-1:659248683008:rqiu_job_archive"
                response = archive_sns_client.publish(
                    TopicArn=topicARN,
                    Message=json.dumps({'default': json.dumps(data_glacier)}),
                    MessageStructure='json')



    else:
        print("A valid .vcf file must be provided as input to this program.")
