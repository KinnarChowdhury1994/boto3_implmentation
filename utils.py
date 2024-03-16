from datetime import datetime
import logging
from logging.handlers import (RotatingFileHandler)
from constants import ACCESSKEY,SECRETKEY,REGION
import boto3
from boto3.exceptions import Boto3Error

formatter = logging.Formatter('%(asctime)s %(name)-8s %(module)s %(lineno)d %(levelname)-8s %(message)s')
def setup_logger(name, log_file, level="DEBUG"):
    handler = RotatingFileHandler(log_file, maxBytes=120000, backupCount=1, delay=False)
    handler.setFormatter(formatter)
    log = logging.getLogger(name)
    log.setLevel(level)
    log.addHandler(handler)
    return log

# first file log
log = setup_logger('LOG', 'utils.log')


def connect_with_boto3(reference):
    AWSACCESSKEY = ACCESSKEY
    AWSSECRETKEY = SECRETKEY
    AWSREGION = REGION
    log.warning(f'Reference = {reference}')
    botoclient = boto3.client(f'{reference}', region_name=AWSREGION,aws_access_key_id=AWSACCESSKEY, aws_secret_access_key=AWSSECRETKEY)
    return botoclient


def s3_connect():
    return connect_with_boto3(reference="s3")

""" ==============================   AWS S3 Data Upload/Download/Delete Operation ========================"""
def list_s3_bucket():
    s3 = s3_connect()
    response = s3.list_buckets()
    return response

def cs3():
    client = boto3.client('s3')
    create_bucket = client.create_bucket(Bucket="test-kinnar-boto")
    log.info(create_bucket)

def create_s3_bucket(bucket_name):
    """
    response = s3.create_bucket(
        ACL='private'|'public-read'|'public-read-write'|'authenticated-read',
        Bucket='string',
        CreateBucketConfiguration={
            'LocationConstraint': 'af-south-1'|'ap-east-1'|'ap-northeast-1'|'ap-northeast-2'|'ap-northeast-3'|'ap-south-1'|'ap-south-2'|'ap-southeast-1'|'ap-southeast-2'|'ap-southeast-3'|'ca-central-1'|'cn-north-1'|'cn-northwest-1'|'EU'|'eu-central-1'|'eu-north-1'|'eu-south-1'|'eu-south-2'|'eu-west-1'|'eu-west-2'|'eu-west-3'|'me-south-1'|'sa-east-1'|'us-east-2'|'us-gov-east-1'|'us-gov-west-1'|'us-west-1'|'us-west-2',
            'Location': {
                'Type': 'AvailabilityZone',
                'Name': 'string'
            },
            'Bucket': {
                'DataRedundancy': 'SingleAvailabilityZone',
                'Type': 'Directory'
            }
        },
        GrantFullControl='string',
        GrantRead='string',
        GrantReadACP='string',
        GrantWrite='string',
        GrantWriteACP='string',
        ObjectLockEnabledForBucket=True|False,
        ObjectOwnership='BucketOwnerPreferred'|'ObjectWriter'|'BucketOwnerEnforced'
    )
    """
    s3 = s3_connect()
    try:
        response = s3.create_bucket(Bucket=bucket_name)
        log.warning(f'RESPONSE of create bucket -\n{response}')
    except Boto3Error as ce:
        log.exception(ce)
        response = "BOTO CLIENT ERROR"
    except Exception as e:
        log.exception(e)
        response = "ERROR" 
    finally:
        return response
    

def upload_file_s3(bucket_name, object_name, file_name):
    """
        Upload image file to S3
    """
    status = 0
    S3 = s3_connect()
    try:
        with open(file_name, 'rb') as f:
            S3.upload_fileobj(f, bucket_name, object_name)
            log.info("File uploaded successfully")
            status = 1
    
    except boto3.ClientError as ce:
        log.exception(ce)
        status = 0

    except Exception as e:
        log.exception(e)
        status = 0
        
    finally:
        return status

def download_file_s3(bucket_name, object_name, file_name):
    """ 
        Download file from S3
    """
    status = 0
    try:
        status = 0
        S3 = s3_connect()
        with open(file_name, 'wb') as f:
            S3.download_fileobj(bucket_name, object_name, f)
            log.info("File downloaded successfully")
            status = 1
            
    except boto3.ClientError as ce:
        log.exception(ce)
        status = 0
        
    except Exception as e:
        log.exception(e)
        status = 0
        
    finally:
        return status
        
def delete_file_s3(bucket_name, object_name):
    """ 
        Delete a single object/file from S3 Bucket
    """
    try:
        status = 0
        S3 = s3_connect()
        log.warning(f"Deleting {object_name} from s3 .......")
        a = S3.delete_object(Bucket=bucket_name, Key=object_name)
        log.info(a)
        log.info("Object/File deleted successfully.")
        status = 1
        
    except boto3.ClientError as ce:
        log.exception(ce)
        status = 0
        
    except Exception as e:
        log.exception(e)
        status = 0
        
    finally:
        return status


if (__name__ == "__main__"):
    cs3()