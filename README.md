# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)). Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* /web: views.py and config.py files and other files necessary for web server to run with
- /web/templates: all HTML templates
- /ann: annotator.py, run.py, ann_config.ini, and all other AnnTools files
- /util: util_config.ini, helpers.py, ann_load.py
- /util/archive: archive.py, archive_config.ini, other archive related
code
- /util/restore: restore.py, restore_config.ini, other restore related
code
- /util/thaw: thaw.py, thaw_config.ini, other thaw related code
- /aws: user_data_web_server.txt, user_data_annotator.txt


The archive process:
    When a user submit a job, web server sends a notification to the requests queue with the user tier status enclosed in the message. When the annotator receives the message, it pass this tier status to run.py. Once run.py finishes the job, it checks this user status. If it is "free_user", then it sends out a notification to the archive queue. The queue is set with a 5 min delay. It sends out the message to the archive.py 5 min after receiving this message from run.py. util file archive.py receives this message. It then downloads the result file from S3 result bucket, proceeds to archive the result file to Glacier, then delete the file both on S3 bucket and the local directive. 


The restore process:
    When a user submit a job, the user tier status is recorded in the DynamoDB such that every job has a record of user status(status of the user when they submit the job). When a user upgrade, views.py sends out a notification to the upgrade queue. Restore.py listens to the queue and retrieve all the job associated with this user. For job with premium users status, we do not need to restore as they were never archived. For job with free user status, restore.py initiate the retrieval process and sends out a notification to restore init queue. This queue is listened by thaw.py. When thaw.py receives a message, it starts to constantly checks the status of the retrieval job and once the retrieval is finished, it download the result file from glacier and upload to S3. 

