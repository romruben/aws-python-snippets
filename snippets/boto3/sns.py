import os

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")

class SNSHandler(object):
    """
    Used to send notificatios through SNS.
    """

    def __init__(self, session: Session = None):
        """
        SNS constructor
        :param session: Session to assign in sns client
        """
        self.client = boto3.client('sns', session=session)
        self.resource = boto3.resource('sns')

    def send_notification(self, topic: str, subject: str, message: str) -> None:
        """
        Send notification message using a topic
        :param topic: message topic
        :param subject: subject title
        :param message: message body
        """
        self.client.publish(TopicArn=topic, Message=message, Subject=subject)
