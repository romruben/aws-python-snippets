"""
Module to handle db connections and transacctions
connect is a function used by contextmanager, transaction. Transaction is a decorator that handle transactionality
in db ops
"""
import logging
import os
from contextlib import contextmanager
from os import getenv

import psycopg2
from boto3.session import Session
from botocore.exceptions import ClientError
from psycopg2._psycopg import connection

import boto3

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


def redshift_connection(database: str, url: str, port: int, user: str, psswd: str) -> connection:
    """
    Function that connects with Redshift using psycopg2 library
    :return:
    """
    secret_manager = SecretManagerHandler()
    password = secret_manager.get_secret(psswd)
    try:
        return psycopg2.connect(dbname=database, host=url, port=port, user=user, password=password)

    except psycopg2.Error as e:
        logger.error(
            f"Error connecting to database {database} with url {url}:{port} and user {user}")
        raise e


@contextmanager
def transaction() -> None:
    """
    Execute transaction using a connection
    :return: todo
    """
    conn = None
    # todo read redshift_connection in contextmanager by env variables or otherwise passing by value
    try:
        conn = redshift_connection(
            getenv('REDSHIFT_DATABASE'),
            getenv('REDSHIFT_URL'),
            getenv('REDSHIFT_PORT'),
            getenv('REDSHIFT_USER'),
            getenv('REDSHIFT_PASSWD')
        )
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise e
    finally:
        if conn:
            conn.close()
