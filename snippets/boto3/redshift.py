"""
This file contains all method related with redshift operations needed for the project
"""

from datetime import datetime
from json import dumps
from urllib.parse import unquote

from awslib.db import transaction


class RedshiftHandler(object):
    """
    Class to operate with Redshift database
    """

    def __init__(self):
        """
        Redshift Handler constructor
        constructor
        """
        self.s3_handler = SThreeHandler()

    @staticmethod
    def build_manifest(bucket: str, objects: [SThreeObject]) -> dict:
        """
        Build manifest from s3 keys
        :param objects: s3 objects
        :param bucket: bucket name
        :return: manifest content
        """
        return {
            "entries": [
                {"url": "s3://{}/{}".format(bucket, unquote(obj.key)), "mandatory": True, "meta": {
                    "content_length": obj.size
                }} for obj in objects]
        }

    def upload_manifest(self, bucket: str, manifest: dict) -> str:
        """
        Upload manifest into manifestPaths
        :param bucket: bucket name
        :param manifest: manifest local path
        :return:
        """
        now = datetime.now()
        key = "loads/manifests/load_{}/{}_load.manifest".format(now.strftime("%d%m%y"),
                                                                now.strftime("%H%M%S%f"))
        self.s3_handler.upload_object(bucket, key, dumps(manifest))
        return key

    @staticmethod
    def execute_async_transaction(queries: [str]) -> None:
        """
        Execute a transaction in Redshift
        :param queries: a list of queries
        :return: todo
        """
        with transaction() as conn:
            for query in queries:
                conn.cursor().execute(query)
