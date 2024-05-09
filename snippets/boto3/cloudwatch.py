import json
import os

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")


class CloudWatchHandler(object):

    def __init__(self, session: Session = None):
        self.client = (session if session else boto3).client('events', region_name=AWS_REGION)
        self._config = {}

    def _get_target_id(self, rule_name):
        return "check-job_" + rule_name

    def create_cw_rule_for_emr_job(self, cluster_id, lambda_arn):
        event_pattern = {
            "source": ["aws.emr"],
            "detail-type": ["EMR Step Status Change"],
            "detail": {
                "state": [
                    "CANCEL_PENDING",
                    "CANCELLED",
                    "FAILED",
                    "COMPLETED"
                ],
                "clusterId": [
                    cluster_id
                ]
            }
        }
        rule_name = "EMRJobStatus-Rule-" + cluster_id
        result = self.client.put_rule(Name=rule_name, EventPattern=json.dumps(event_pattern),
                                      State="ENABLED",
                                      Description="Event for EMR cluster " + cluster_id)
        lambda_target = {"Id": self._get_target_id(rule_name), "Arn": lambda_arn}
        self.client.put_targets(Rule=rule_name, Targets=[lambda_target])
        return rule_name, result['RuleArn']

    def remove_cw_rule(self, rule_name):
        try:
            self.client.remove_targets(Rule=rule_name, Ids=[self._get_target_id(rule_name)])
            return self.client.delete_rule(Name=rule_name)
        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Rule {rule_name} could not be removed because it does not exist")
            raise e

    def exists_rule(self, name):
        print(f"Checking if exists rule {name}")
        return len(self.client.list_rules(NamePrefix=name)['Rules']) == 1

    def get_rule(self, rule_name):
        return self.client.describe_rule(Name=rule_name)
