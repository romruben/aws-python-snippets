"""
File that uses secret manager service to recover secrets stored and use in the project.
"""
import logging
import os

from botocore.exceptions import ClientError

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class SecretManagerHandler(object):
    """
    Class to operate with Secret Manager service
    """

    def __init__(self, session: Session = None):
        """
        Secret Manager Handler constructor
        """
        self.client = (session if session else boto3).client('secretsmanager',
                                                             region_name=AWS_REGION)

    def get_secret(self, secret: str) -> str:
        """
        Gets secret stored in Secret Manager service
        :param secret: secret to recover
        :return:
        """

        try:
            get_secret_value_response = self.client.get_secret_value(SecretId=secret)

            if 'SecretString' in get_secret_value_response:
                return get_secret_value_response['SecretString']
            else:
                logger.error('Secret is not a string, invalid secret content')
                return ""

        except ClientError as e:
            raise Exception()
