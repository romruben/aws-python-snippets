"""
Dynamodb.py file contains all the functions needed to handle operations with dynamo database.
"""
import logging
import os
from decimal import Decimal
from json import JSONEncoder, dumps

from botocore.exceptions import ClientError

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class DecimalEncoder(JSONEncoder):
    """
    Helper class to convert a DynamoDB item to JSON.
    """

    def default(self, o):
        """
        checks cast to float if o is Decimal, otherwise int
        :param o:
        :return:
        """
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DynamoHandler(object):
    """
    Class to operate with Dynamodb service
    """

    def __init__(self, session: Session = None):
        """
        Dynamodb Handler constructor
        """
        self.client = (session if session else boto3).client('dynamodb', region_name=AWS_REGION)
        self.resource = (session if session else boto3).resource('dynamodb', region_name=AWS_REGION)

    def get_element(self, table_name: str, item_key: dict):
        """
        Gets the element which item_key is stored in table_name
        :param table_name:
        :param item_key:
        :return:
        """
        try:
            response = self.resource.Table(table_name).get_item(Key=item_key)
            if 'Item' in response:
                return response['Item']
            else:
                raise Exception(f"Item key {item_key} not found in {table_name}")
        except ClientError as ex:
            logger.error(f"Exception: {ex} in table {table_name} with key {dumps(item_key)}")
            raise ex

    def scan_table(self, table_name: str, limit=None):
        """
        Scan all elements from table
        :param limit: page max size
        :param table_name:
        :return:
        """
        try:
            table = self.resource.Table(table_name)

            result = table.scan(Limit=limit) if limit else table.scan()
            items = result['Items']

            while 'LastEvaluatedKey' in result:
                result = table.scan(ExclusiveStartKey=result['LastEvaluatedKey'])
                items.extend(result['Items'])
            return items
        except Exception as ex:
            logger.error(f'Exception in table {table_name}')
            raise ex

    def put_elements(self, table_name: str, items: [dict]) -> None:
        """
        Put multiple elements in dynamoDB table in batch mode
        :param table_name: tablename
        :param items: items to put in dynamodb
        :return:
        """
        try:
            table = self.resource.Table(table_name)
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
        except Exception as e:
            itemslen = str(len(items))
            logger.error(f'Exception with table_name {table_name} and item size {itemslen}')
            raise e

    def put_element(self, table_name: str, item: dict) -> dict:
        """
        Put multiple elements in dynamoDB table in batch mode
        :param item:
        :param table_name: tablename
        :return:
        """
        try:
            return self.resource.Table(table_name).put_item(Item=item)
        except Exception as e:
            logger.error(f'Exception with table_name {table_name}')
            raise e

    def delete_item(self, table: str, item_key: dict):
        """
        Delete item from dynamodb table based on item_key
        :param table: tablename
        :param item_key: {key:value}
        :return:
        """
        try:
            response = self.resource.Table(table).delete_item(Key=item_key)

            if 'Attributes' in response:
                logger.error(f'Key {item_key.values()} not found in table name {table}')
            return response

        except Exception as e:
            logger.error(f'Exception with table name {table}')
            raise e

    def update_throughput(self, table_name: str, read_capacity_units: int,
                          write_capacity_units: int) -> None:
        """
        Updates throughput of write and read for a dynamodb table
        :param table_name: dynamodb table name
        :param read_capacity_units: read capacity to set
        :param write_capacity_units: write capacity to set
        :param kwargs:
        :return:
        """
        try:
            table = self.resource.Table(table_name)
            current_throughput = table.provisioned_throughput

            if current_throughput['ReadCapacityUnits'] != read_capacity_units or \
                    current_throughput['WriteCapacityUnits'] != write_capacity_units:
                table.update(
                    ProvisionedThroughput={
                        'ReadCapacityUnits': read_capacity_units,
                        'WriteCapacityUnits': write_capacity_units
                    }
                )

                waiter = self.client.get_waiter('table_exists')
                waiter.wait(
                    TableName=table_name,
                    WaiterConfig={
                        'Delay': 20,
                        'MaxAttempts': 10
                    }
                )
        except Exception as _:
            logger.warning(f'Exception with table name {table_name}')
