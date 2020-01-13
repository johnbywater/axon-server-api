from unittest import TestCase
from uuid import uuid4

from axonclient.client import AxonClient, AxonClientError


class TestAxonClient(TestCase):
    def test_failing_to_connect_raises_exception(self):
        uri = 'localhost:50051'
        client = AxonClient(uri)
        aggregate_id = uuid4()
        with self.assertRaises(AxonClientError):
            client.list_aggregate_events(aggregate_id)
