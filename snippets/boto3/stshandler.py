import logging
import os

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class STSHandler(object):

    def __init__(self, session: boto3):
        self.client = (session if session else boto3).client('sts', region_name=AWS_REGION)

    def role_arn_to_session(self, role_arn, session_name="sts_session") -> Session:
        """
        Usage :
            session = role_arn_to_session(
                RoleArn='arn:aws:iam::012345678901:role/example-role',
                RoleSessionName='ExampleSessionName')
            client = session.client('sqs')
        """
        response = self.client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)

        return boto3.Session(
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken'])
