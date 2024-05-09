import unittest
from unittest import mock

from snippets.boto3.athena import AthenaHandler


@mock.patch("snippets.boto3.athena.AWS_REGION", "eu-west-1")
class AWSAthenaUnitTests(unittest.TestCase):

    def test_client_creation(self):
        client = AthenaHandler()
        actual = 'Athena'
        expected = str(client.client.__class__.__name__)
        assert actual == expected

    def test_generate_repair_all_partitions_query(self):
        athena = AthenaHandler()
        actual = 'MSCK REPAIR TABLE test_table;'
        expected = str(athena.generate_repair_all_partitions_query("test_table"))
        assert actual == expected

    def test_generate_day_partition_query(self):
        athena = AthenaHandler()
        partitions = ["year=2019", "month=01", "day=01"]
        expected = "ALTER TABLE test_table ADD IF NOT EXISTS PARTITION (year=2019, month=01, day=01) location \'.\';"
        actual = athena.generate_day_partition_query(partitions, "test_table", ".")
        assert actual == expected
