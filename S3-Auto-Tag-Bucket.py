import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# This function gets invoked by CloudTrail rule for CreateBucket event.
def lambda_handler(event, context):
    s3_client = boto3.resource('s3')

    region = event['region']
    detail = event['detail']
    eventname = detail['eventName']

    if eventname == 'CreateBucket':
        try:
            bucket_name = detail['requestParameters']['bucketName']
            s3_client.BucketTagging(bucket_name).put(Tagging={'TagSet': [{'Key':'Name','Value': bucket_name}]})
        except Exception as e:
            logger.error('Exception thrown at CreateBucket' + str(e))
            pass