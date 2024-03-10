# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
import os
import uuid
import time
import json
from datetime import datetime,timedelta
import configparser
import boto3
import requests
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

config = configparser.ConfigParser()
config.read('web_config.ini')
dynamo = boto3.resource('dynamodb')
table_name = config['Table']['table']
table = dynamo.Table(table_name)
"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']
  
  
  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
  bucket = request.args.get('bucket')
  key = request.args.get('key')
  key1 = str(os.path.basename(key))
  id_name = key1.split("~")
  id = id_name[0]
  filename = id_name[1]

  # Current time in UTC
  current_utc_datetime = datetime.utcnow()

  # Convert to epoch time in seconds
  epoch = int(current_utc_datetime.timestamp())
  user_id =  session['primary_identity']
  profile = get_profile(identity_id=session.get('primary_identity'))
  # We need to pass the email, and user status to the db as well for later operations
  user_email = profile.email
  data = { "job_id": id,
            "user_id": user_id,
          "input_file_name":  filename,
          "s3_inputs_bucket": bucket,
          "s3_key_input_file": key,
          "submit_time": epoch,
          "job_status": "PENDING",
          "email": user_email,
          "user_status": session['role']
          }
  
  response = table.put_item(Item = data)
  print(f"Successfully wrote job ID {data['job_id']}")
  
  client = boto3.client("sns")
  # Your SNS topic ARN (Amazon Resource Name)
  
  topicARN = "arn:aws:sns:us-east-1:659248683008:rqiu_job_requests"
  response = client.publish(
      TopicArn=topicARN,
      Message=json.dumps({'default': json.dumps(data)}),
      MessageStructure='json')

  # Extract the job ID from the S3 key

  # Send message to request queue

  return render_template('annotate_confirm.html', job_id=id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  annotations=[]
  # Get list of annotations to display
  # Fetch the current user and his/her jobs info from db
  # Then pass these information to the annotation html
  response = table.query(
    IndexName='user_id_index',  
    KeyConditionExpression=Key('user_id').eq(session['primary_identity'])
  )
  items = response['Items']
  
  for item in items:
    timestamp = item['submit_time']
    dt_object = datetime.utcfromtimestamp(timestamp)
    formatted_date = dt_object.strftime('%Y-%m-%d %H:%M')
    annotation = {
      "job_id": item['job_id'],
      "submit_time": formatted_date,
      "input_file_name": item['input_file_name'],
      "job_status": item['job_status']
    }
    annotations.append(annotation)
  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
# Fetch the info of the job from the db
# Generate presigned download url using S3
# Reference: Sharing objects with presigned URLs,  https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
# If the completion time exceeds current time by 5 min, set free access to false
def annotation_details(id):
  free_access_expired = False
  s3_client = boto3.client('s3')
  response = table.query(
    KeyConditionExpression=Key('job_id').eq(id)
  )
  items = response['Items']
  for item in items:
    download_url = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                                                            'Key': item['s3_key_input_file']},
                                                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    object_name = os.path.basename(item['s3_key_input_file'])
    object_name = "rqiu/" + object_name[:-4] + ".annot.vcf"
    download_result_url = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                            'Key': object_name},
                                                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    timestamp = item['submit_time']
    dt_object = datetime.utcfromtimestamp(timestamp)
    time_submit = dt_object.strftime('%Y-%m-%d %H:%M')
    
    if item['job_status'] == 'COMPLETED':
      timestamp_dec = item['complete_time']
      dt_object = datetime.utcfromtimestamp(timestamp_dec)
      time_complete = dt_object.strftime('%Y-%m-%d %H:%M')
      # Here we want to check if the presigned url is valid because the file may not yet be restored to bucket
      test_file_exist_response = requests.get(download_result_url)
      status_code_exist = test_file_exist_response.status_code
      if status_code_exist == 200:
        annotation = {
          'user_id': session['primary_identity'],
          'owner_id': item['user_id'],
          'input_file_name': item['input_file_name'],
          'input_file_url': download_url,
          'job_status': item['job_status'],
          'complete_time': time_complete,
          'result_file_url': download_result_url,
          'job_id': id,
          'submit_time': time_submit
        }
      else:
        annotation = {
          'user_id': session['primary_identity'],
          'owner_id': item['user_id'],
          'input_file_name': item['input_file_name'],
          'input_file_url': download_url,
          'job_status': item['job_status'],
          'complete_time': time_complete,
          'restore_message': "Restoring your file from S3 Glacier. This could take about 5 hours. Please check back later",
          'job_id': id,
          'submit_time': time_submit
        }
      if session['role'] == 'free_user':
          if datetime.now() - dt_object > timedelta(seconds = 300):
            free_access_expired = True
    else:
      annotation = {
        'user_id': session['primary_identity'],
        'owner_id': item['user_id'],
        'input_file_name': item['input_file_name'],
        'input_file_url': download_url,
        'job_status': item['job_status'],
        'job_id': id,
        'submit_time': time_submit
      }
  return render_template('annotation_details.html', annotation=annotation, free_access_expired = free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
# Fetch the job info from db
# Download the log file from S3
# Pass the contents to view log html and delete the downloaded file
def annotation_log(id):
  response = table.query(
    KeyConditionExpression=Key('job_id').eq(id)
  )
  items = response['Items']
  s3_client = boto3.client('s3')
  for item in items:
    object_name = os.path.basename(item['s3_key_input_file'])
    key = "rqiu/" + object_name + ".count.log"
    local_file_name = object_name + ".count.log"
    s3_client.download_file(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=key, Filename=local_file_name)
    with open(local_file_name, 'r') as file:
      log_file_contents = file.read()
    if os.path.exists(local_file_name):
                os.remove(local_file_name)
  return render_template('view_log.html', job_id = id, log_file_contents = log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # We only pass the user Id here to the upgrade queue
    # restore.py will handle retrieving the right file
    data = { "user_id": session['primary_identity'],
          }
  
    # Publish a message to the upgrade topic
    sns = boto3.client("sns")
    
    topicARN = "arn:aws:sns:us-east-1:659248683008:rqiu_job_upgrade"
    response = sns.publish(
        TopicArn=topicARN,
        Message=json.dumps({'default': json.dumps(data)}),
        MessageStructure='json')

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
