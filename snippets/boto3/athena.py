import os

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")


class AthenaHandler(object):
    def __init__(self, session: Session = None):
        self.client = (session if session else boto3).client('athena', region_name=AWS_REGION)

    def run_query(self, query: str, database: str):
        return self.client.start_query_execution(QueryString=query,
                                                 QueryExecutionContext={'Database': database},
                                                 WorkGroup="primary")

    def generate_repair_all_partitions_query(self, table: str):
        return f"MSCK REPAIR TABLE {table};"

    def generate_day_partition_query(self, partitions: list[str], table_name: str,
                                     location: str = '', ):
        partition_fmt = f"({', '.join(partitions)})"
        location = f"location '{location}'"

        return f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION {partition_fmt} {location};"
