"""
Contains all methods needed for use simple storage service in this project
"""
import logging
import multiprocessing
import os

import botocore

import boto3
from boto3 import Session

AWS_REGION = os.getenv("AWS_REGION", "<YOUR_REGION>")
logger = logging.getLogger()


class SThreeObject(object):
    """
    S3 Object Representation
    """

    def __init__(self, meta, content=None) -> None:
        """
        SThreeObject constructor
        :param meta: file metadata obtained from S3 API
        2019-08-29 11:11:07.977000+00:00
        """

        self.key = meta['Key']
        self.last_modified = meta['LastModified']
        self.etag = meta['ETag']
        self.size = meta['Size']
        self.storage_class = meta['StorageClass']
        self.content = content

    def __eq__(self, other) -> bool:
        """
        Check if two sThreeObjects are the same
        :param other: object with compare to
        :return:
        """
        return self.etag == other.etag


class SThreeHandler(object):
    """
    Class to operate with Simple Storage Service
    """

    def __init__(self, session: Session = None):
        """
        S3 Handler constructor
        """
        self.client = (session if session else boto3).client('s3', region_name=AWS_REGION)
        self.resource = (session if session else boto3).resource('s3', region_name=AWS_REGION)

    def upload_file(self, bucket_name: str, key: str, file_name: str) -> None:
        """
        Upload local file to specified bucket and key
        :param bucket_name:
        :param key:
        :param file_name:
        :return:
        """
        try:
            logger.debug("Uploading file {} to s3://{}/{}".format(file_name, bucket_name, key))
            self.client.upload_file(file_name, bucket_name, key)
        except Exception as e:
            logger.error(
                f'Exception with bucket_name {bucket_name} , key {key} and file_name {file_name}')
            raise e

    def upload_object(self, bucket_name: str, key: str, content: str) -> dict:
        """
        Upload object to specified bucket and key
        :param bucket_name:
        :param key: key path
        :param content: body content
        :return: api response of upload_object request
        """
        try:
            logger.debug("Uploading object to s3://{}/{}".format(bucket_name, key))
            return self.client.put_object(Bucket=bucket_name, Key=key, Body=content)
        except Exception as e:
            logger.error(f'Exception uploading object in bucket_name {bucket_name} with key {key}')
            raise e

    def download_file(self, bucket_name: str, key: str, file_name: str) -> str:
        """
        Download file if exists from specified bucket and key to local file_name
        :param bucket_name: bucket name
        :param key: object key
        :param file_name: target local path
        :return: local path in which the file is stored
        """
        try:
            logger.debug("Downloading file {} from s3://{}/{}".format(file_name, bucket_name, key))
            self.client.download_file(bucket_name, key, file_name)
            return file_name
        except Exception as e:
            logger.error(f'Exception downloading file s3://{bucket_name}/{key} into {file_name}')
            raise e

    def get_object_metadata(self, bucket_name: str, key: str) -> SThreeObject:
        """
        Get object if exists
        :param bucket_name: bucket name
        :param key: object key
        :return: SThreeObject
        """
        try:
            logger.debug("Getting object {} from s3://{}".format(bucket_name, key))
            obj = list(self.list_objects(bucket_name, key))[0]
            return obj
        except Exception as e:
            logger.error(f'Exception getting object of bucket_name {bucket_name} with key {key}')
            raise e

    def get_object_content(self, bucket_name: str, key: str) -> str:
        """
        Get object if exists from specified bucket and key
        :param bucket_name: bucket name
        :param key: object key
        :return: object content in utf-8 encoding
        """
        try:
            logger.debug("Getting object {} from s3://{}".format(bucket_name, key))
            obj = self.resource.Object(bucket_name, key)
            return obj.get()['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f'Exception getting object of bucket_name {bucket_name} with key {key}')
            raise e

    def list_objects(self, bucket_name: str, prefix='', suffix='') -> [SThreeObject]:
        """
        Generate the keys in an S3 bucket.

        :param bucket_name: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
        while True:
            resp = self.client.list_objects_v2(**kwargs)
            for obj in resp['Contents'] if 'Contents' in resp else []:
                key = obj['Key']
                if key.endswith(suffix):
                    yield SThreeObject(obj)
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def exists(self, bucket_name: str, key: str) -> bool:
        """
        Checks if a file exists
        :param bucket_name: bucket name
        :param key: s3 file key
        :return:
        """
        try:
            logger.debug(f"Checking if exists the following object s3://{bucket_name}/{key}")
            obj = self.resource.Object(bucket_name, key)
            logger.debug(f'Object e_tag {obj.e_tag}')
            return True
        except botocore.exceptions.ClientError as _:
            logger.debug("Object doesn't exists")

        return False

    def get_object_etag(self, bucket_name: str, key: str) -> SThreeObject:
        """
        Gets object etag md5
        :param bucket_name: bucket name
        :param key: s3 file key
        :return:
        """
        try:
            logger.debug(f"Retrieving object etag s3://{bucket_name}/{key}")
            obj = self.resource.Object(bucket_name, key)
            logger.debug(f'Object e_tag {obj.e_tag}')
            return obj.e_tag
        except botocore.exceptions.ClientError as _:
            raise Exception("Object doesn't exists")

    def objects_are_equals(self, from_object: dict, to_object: dict) -> bool:

        """
        Check if files are equals
        :param from_bucket:
        :param from_key:
        :param to_bucket:
        :param to_key:
        :return:
        """
        return self.exists(from_object['Bucket'], from_object['Key']) and \
            self.exists(to_object['Bucket'], to_object['Key']) and \
            self.get_object_etag(from_object['Bucket'], from_object['Key']) == self.get_object_etag(
                to_object['Bucket'],
                to_object['Key'])

    def bulk_copy(self, from_to: list) -> bool:
        """
        Bulk copy files.
        :param from_to: A list of tuples, the first element is a dict with origin details (bucket and key) and the
        second one contains the target path.
        :return:
        """
        for (_from, _to) in from_to:
            self.resource.meta.client.copy(_from, _to['Bucket'], _to['Key'])

        return True

    def delete_object(self, bucket_name: str, key: str) -> dict:
        """
        Deletes an object from S3.
        :param bucket_name: bucket name
        :param key: object key
        :return:
        """
        return self.client.delete_object(Bucket=bucket_name, Key=key)

    def _start_multi_part_upload(self, bucket_name: str, key: str) -> str:
        response = self.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )

        return response['UploadId']

    def _add_multipart_part(self, proc_queue, body: str, bucket_name: str, key: str,
                            part_number: int, upload_id: str):
        print("hi")
        response = self.client.upload_part(
            Body=body,
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id
        )

        print(f"Finished Part: {part_number}, ETag: {response['ETag']}")
        proc_queue.put({'PartNumber': part_number, 'ETag': response['ETag']})

    def _end_multipart_upload(self, bucket_name: str, key: str, upload_id: str,
                              finished_parts: list) -> dict:
        response = self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            MultipartUpload={
                'Parts': finished_parts
            },
            UploadId=upload_id
        )

        return response

    def multipart_upload(self, file_path: str, key: str, bucket_name: str, chunk_size: int,
                         processes: int = 10) -> dict:
        upload_id = self._start_multi_part_upload(bucket_name, key)
        file_upload = open(file_path, 'rb')
        chunk_size = (chunk_size * 1024 * 1024)

        part_procs = []
        proc_queue = multiprocessing.Queue()
        queue_returns = []
        part_num = 1
        chunk = file_upload.read(chunk_size)

        while len(chunk) > 0:
            proc = multiprocessing.Process(target=self._add_multipart_part,
                                           args=(proc_queue, chunk, bucket_name, key, part_num,
                                                 upload_id))
            part_procs.append(proc)
            part_num += 1
            chunk = file_upload.read(chunk_size)

        part_procs = [part_procs[i * processes:(i + 1) * processes] for i in
                      range((len(part_procs) + (processes - 1)) // processes)]

        for i in range(len(part_procs)):
            for p in part_procs[i]:
                p.start()

            for p in part_procs[i]:
                p.join()

            for _ in part_procs[i]:
                queue_returns.append(proc_queue.get())

        queue_returns = sorted(queue_returns, key=lambda i: i['PartNumber'])
        response = self._end_multipart_upload(bucket_name, key, upload_id, queue_returns)
        return response
