import os

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")


class GlueHandler(object):
    def __init__(self, session: Session = None):
        self.client = (session if session else boto3).client('glue', region_name=AWS_REGION)

    def update_crawler_paths(self, crawler_name: str, path: str) -> None:
        """
        Include new paths to crawler
        :param glue_client: glue boto3 client
        :param crawler_name: crawler to update
        :param path: new path to add
        """
        targets = self.client.get_crawler(Name=crawler_name)['Crawler']['Targets']['S3Targets']
        path_to_add = [{'Path': path, 'Exclusions': []}]
        self.client.update_crawler(Name=crawler_name, Targets={'S3Targets': targets + path_to_add})

    def replace_crawlers_s3_targets(self, crawler_name: str, paths: list) -> None:
        """
        Update target for s3 paths
        :param crawler_name: crawler name
        :param paths: s3 paths to set
        """
        targets = [{'Path': path, 'Exclusions': []} for path in paths]
        self.client.update_crawler(Name=crawler_name, Targets={'S3Targets': targets})

    def get_s3_crawler_paths(self, crawler_name: str) -> list:
        """
        Returns crawler paths defined
        :param crawler_name: crawler name
        :return: a list of crawler paths
        """
        targets = self.client.get_crawler(Name=crawler_name)['Crawler']['Targets']['S3Targets']
        return [target['Path'] for target in targets]
