#Description: Function to initiate Glacier Select job. Glacier Select will store the output in the specified S3 bucket  
#Runtime:python 3.7 
#Assumptions: Glacier retrieval job parameters included "Standard" tier. Hence, it may take up to 3 to 5 hours to see the result.For faster #retrieval Expedited option can be considered
#Inputs/ Environment variables - Archiveid, Glacier vault name, output S3 bucket and prefix name

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
archiveid = os.getenv('archiveid')
vault = os.getenv('vaultname')
s3bucket= os.getenv('bucketname')
s3prefix= os.getenv('prefix')


glacier = boto3.client('glacier')

logger.info('starting Lambda')

def lambda_handler(event, context):

    
   #glacier block
     
    logger.info('starting glacier block')
    print(archiveid)
    try:
		jobParameters = {"Type": "select", "ArchiveId": archiveid,"Tier": "Standard", "SelectParameters": {
        "InputSerialization": {"csv": {}},
        "ExpressionType": "SQL",
        "Expression": "select s._1,s._2,s._3,s._4,s._5,s._12,s._13 from archive s  where CAST(s._2 AS DECIMAL) >= 500000",
        "OutputSerialization": {
            "csv": {}
        }
		},
		"OutputLocation": {
        "S3": {"BucketName": s3bucket, "Prefix":s3prefix}
		}
		}
		response=glacier.initiate_job(vaultName=vault, jobParameters=jobParameters)
        
    except Exception as e:
      print(e)
      print("Error selecting data from archive ", archiveid, "from vault ", vault, "to bucket",s3bucket)
      raise e  
    logger.info('downloaded data from glacier archive')
    print(response)
    return response
