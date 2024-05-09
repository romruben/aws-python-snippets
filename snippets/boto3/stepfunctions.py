import json
import logging
import os

from botocore.client import Config
from botocore.exceptions import ClientError

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()

class StepFunctionsHandler(object):
    # TODO: what about step functions heartbeat.

    def __init__(self, session: Session = None):
        sfn_client_config = Config(connect_timeout=50, read_timeout=70)
        self._sfn = boto3.client('stepfunctions', config=sfn_client_config, region_name=AWS_REGION,
                                 session=session)

    def task_success(self, task_token, output="{}"):
        self._sfn.send_task_success(taskToken=task_token, output=output)

    def task_failure(self, task_token, cause="cause of error"):
        self._sfn.send_task_failure(taskToken=task_token, cause=cause, error="EMRJobError")

    def get_tasks(self, activity_arn, worker_name):
        tasks = []
        while True:
            response = self._sfn.get_activity_task(activityArn=activity_arn, workerName=worker_name)
            if response.get('taskToken') is None:
                return tasks
            tasks.append(response)

    def run_step_function_machine(self, state_machine_arn, event: dict) -> str:
        """
        Performs a new step machine execution
        """
        try:
            print("Running a new step machine execution...")
            step_function_input = json.dumps(event)

            response = self._sfn.start_execution(
                stateMachineArn=state_machine_arn,
                input=step_function_input
            )

            execution_id = response['executionArn'].split(":")[-1]

            return execution_id

        except ClientError as ce:
            print(f"Error running solve step machine with job {event.get('opJobId')}, "
                  f"error: {ce}")
            raise ce
        except Exception as error:
            raise error

    def stop_step_function_execution(self, state_machine_arn: str, error: str, cause: str):

        print(f"Stopping {state_machine_arn} due to {error}. Cause: {cause}")
        try:
            response = client.stop_execution(
                executionArn=state_machine_arn,
                error=error,
                cause=cause
            )
        except ClientError as ce:
            print(f"Error stopping step machine job {state_machine_arn}, "
                  f"error: {ce}")
            raise ce
        except Exception as ex:
            raise ex
