# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* /web: updated views.py and config.py files; I expect that only these two
files have changed relative to the gas-framework repo that you started with. If you
modified other files from the gas-framework include them here and provide a
description of what you changed (and why) in README.md
- /web/templates: all HTML templates
- /ann: annotator.py, run.py, ann_config.ini, and all other AnnTools files
- /util: util_config.ini, helpers.py, ann_load.py
- /util/archive: archive.py, archive_config.ini, other archive related
code
- /util/restore: restore.py, restore_config.ini, other restore related
code
- /util/thaw: thaw.py, thaw_config.ini, other thaw related code
- /aws: user_data_web_server.txt, user_data_annotator.txt


a. Description of your archive process:
    When a user submit a job, web server sends a notification to the requests queue with the user tier status enclosed in the message. When the annotator receives the message, it pass this tier status to run.py. Once run.py finishes the job, it checks this user status. If it is "free_user", then it sends out a notification to the archive queue. The queue is set with a 5 min delay. It sends out the message to the archive.py 5 min after receiving this message from run.py. util file archive.py receives this message. It then downloads the result file from S3 result bucket, proceeds to archive the result file to Glacier, then delete the file both on S3 bucket and the local directive. 


b. Description of your restore process.
    When a user submit a job, the user tier status is recorded in the DynamoDB such that every job has a record of user status(status of the user when they submit the job). When a user upgrade, views.py sends out a notification to the upgrade queue. Restore.py listens to the queue and retrieve all the job associated with this user. For job with premium users status, we do not need to restore as they were never archived. For job with free user status, restore.py initiate the retrieval process and sends out a notification to restore init queue. This queue is listened by thaw.py. When thaw.py receives a message, it starts to constantly checks the status of the retrieval job and once the retrieval is finished, it download the result file from glacier and upload to S3. 


c. Anything else you’d like to share with the grader. Include notes here if you didn’t
manage to complete all the exercises and wish to describe what issues you
encountered that prevented you from doing so.
    Very fun project. I also used configParsor in views with a file called web_config.ini. It is the same as ann_config.ini but since it was used in the web server, I decided to copy it there. 



d. If you completed the optional exercises, include descriptions of what you
observed and why. If you prefer, you may include the auto scaling description and
screenshots in a PDF file. Do not include Word documents.
