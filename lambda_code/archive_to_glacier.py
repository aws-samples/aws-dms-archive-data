#Description: Function that is triggered via S3 Evenet notification and download the S3 file to /tmp and then upload to Glacier as an archive
#Runtime:python 3.7 
#Assumptions: S3 file size should under 512MB as Lambda has a limited /tmp space (Refer:https://docs.aws.amazon.com/lambda/latest/dg/limits.html)
#Inputs/ Environment variable - Glacier Vault name

from __future__ import print_function
import json
import urllib
import boto3
import uuid
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Global parameters
vault = os.getenv('vaultname')
s3 = boto3.client('s3')
glacier = boto3.client('glacier')
logger.info('starting Lambda')
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = \
        urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8'
        )
    logger.info('starting S3 file download')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        download_path = '/tmp/{}'.format(uuid.uuid4())
        s3.download_file(bucket, key, download_path)
    except Exception as e:
        print(e)
        print("Error getting object ", key, "from bucket ", bucket)
        raise e
  
     
    logger.info('starting glacier block')
    
    file = open(download_path,"rb") 
     
    print(vault)
    try:
      response = glacier.upload_archive( accountId='-',archiveDescription='archiving old data from s3', body=file,vaultName=vault)
        
    except Exception as e:
      print(e)
      print("Error uploading archive ", key, "from bucket ", bucket, "to vault",vault)
      raise e  
    logger.info('uploaded archive. Note down the archive_id from the response')
    print(response)
    os.remove(download_path)
    return response
