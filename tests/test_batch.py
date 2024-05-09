import unittest
from unittest import mock
from unittest.mock import patch, Mock

from snippets.boto3.batch import BatchHandler


@mock.patch("snippets.boto3.batch.AWS_REGION", "eu-west-1")
class AWSBatchUnitTests(unittest.TestCase):

    @patch("snippets.boto3.batch.BatchHandler.get_jobs_by_finished_status")
    def test_call_wait_jobs_with_jobs_failed(self, function_called):
        function_called.return_value = ([0], [1, 2])
        batch = BatchHandler()
        batch.client = Mock()
        self.assertRaises(Exception, batch.wait_jobs, [0, 1, 2])

    @patch("snippets.boto3.batch.BatchHandler.get_jobs_by_finished_status")
    def test_call_wait_jobs_with_jobs_success(self, function_called):
        function_called.return_value = ([0, 1], [])
        batch = BatchHandler()
        batch.client = Mock()

        expected = []
        actual = batch.wait_jobs([0, 1], 1)

        assert actual == expected
