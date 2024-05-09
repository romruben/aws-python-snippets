import logging
import os
from json import dumps
from time import sleep

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class BatchHandler(object):

    def __init__(self, session: Session = None):
        self.client = (session if session else boto3).client('batch', region_name=AWS_REGION)

    def submit_job(self, name: str, queue: str, definition: str, dependencies: list = [],
                   command: list = []) -> dict:
        """
        Submit batch Job
        :param name: job name
        :param queue: job queue
        :param definition: job definition name
        :param dependencies: job dependencies
        :param command: override command to launch job
        :return: a value pair of jobId and jobName
        """
        logger.info("Trying to run batch job with: "
                    "jobName={} jobQueue={} jobDefinition={} dependencies:{} command:{} ".format(
            name, queue,
            definition,
            dumps(dependencies),
            command))

        return self.client.submit_job(
            jobName=name,
            jobQueue=queue,
            jobDefinition=definition,
            dependsOn=dependencies,
            containerOverrides={"command": command})

    def get_jobs_by_finished_status(self, jobs_ids: list) -> (list, list):
        """
        Get succeeded jobs and failed of a jobs_list
        :param jobs_ids: jobs id
        :return:
        """
        meta = self.client.describe_jobs(jobs=jobs_ids)
        failed = [job['jobId'] for job in meta['jobs'] if job['status'] == 'FAILED']
        succeeded = [job['jobId'] for job in meta['jobs'] if job['status'] == 'SUCCEEDED']

        return succeeded, failed

    def wait_jobs(self, jobs_ids: list, time_elapsed: int = 30):
        """
        Wait for jobs termination
        :param jobs_ids: a list of batch jobs id
        :param time_elapsed: time to wait each iteration
        :return:
        """
        while len(jobs_ids) > 1:
            succeeded, failed = self.get_jobs_by_finished_status(jobs_ids)

            if len(failed) > 1:
                raise Exception(f"Exception running job/s: {dumps(failed)}")

            for job in succeeded:
                jobs_ids.remove(job)

            sleep(time_elapsed)

        return jobs_ids
