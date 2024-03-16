import os
#! AWS Credentials
ACCESSKEY = str(os.getenv('ACCESS_KEY'))
SECRETKEY = str(os.getenv('SECRET_KEY'))
REGION = str(os.getenv('REGION'))
#! AWS S3 Folder
BUCKET_NAME = 'kinnarchowdhury-devops-batch4'
OBJECTNAME = 'development/'