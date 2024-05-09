import logging
import os
import shutil
import uuid
from os.path import isdir
from typing import Callable, Any

from lambda_multiprocessing import Pool

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class LambdaHandler(object):

    def __init__(self, session: Session = None):
        self.client = (session if session else boto3).client('lambda', region_name=AWS_REGION)

        self.__WORKSPACE_PATH = '/tmp/{}'.format(uuid.uuid1())
        self.__create_workspace()

    def add_permissions_for_cw_rule(self, cw_rule_arn, lambda_name, statement_id):
        return self.client.add_permission(FunctionName=lambda_name, StatementId=statement_id,
                                          Action='lambda:InvokeFunction',
                                          Principal='events.amazonaws.com',
                                          SourceArn=cw_rule_arn)

    def remove_permissions_for_cw_rule(self, lambda_name, statement_id):
        try:
            return self.client.remove_permission(FunctionName=lambda_name, StatementId=statement_id)
        except Exception as ex:
            print("ERROR: " + str(ex))

    def __regenerate_workspace(self):
        self.__WORKSPACE_PATH = '/tmp/{}'.format(uuid.uuid1())
        self.__create_workspace()

    def get_workspace(self):
        if not isdir(self.__WORKSPACE_PATH):
            self.__regenerate_workspace()
        return self.__WORKSPACE_PATH

    def clean_workspace(self):
        shutil.rmtree(self.__WORKSPACE_PATH)

    def __create_workspace(self):
        os.mkdir(self.__WORKSPACE_PATH)

    # Due to lambda limitations by shared memory lack in /dev/shm u can't use multiprocessing.Queue
    # instead u can use the following library throught Pool
    def concurrent_execution(self, function: Callable[[Any], Any], args: list,
                             async_method: bool = False):
        with Pool() as p:
            map_type = p.map_async if async_method else p.map
            return map_type(function, args)
