import http.client
from unittest import TestCase
from uuid import uuid4

from axonclient.client import AxonClient, AxonClientError
from axonclient.common_pb2 import SerializedObject
from axonclient.event_pb2 import Event


class TestAxonClient(TestCase):
    def test_axon_server_is_running(self):
        conn = http.client.HTTPConnection("localhost:8024")
        try:
            conn.request("GET", "/")
            r1 = conn.getresponse()
            self.assertEqual(r1.status, 200, "Axon Server not running?")
        finally:
            conn.close()

    def test_failing_to_connect_raises_exception(self):
        uri = "localhost:81244444"  # wrong port
        client = AxonClient(uri)
        aggregate_id = uuid4()
        with self.assertRaises(AxonClientError):
            client.list_aggregate_events(aggregate_id)

    def test_append_and_list_aggregate_events(self):
        uri = "localhost:8124"
        client = AxonClient(uri)
        aggregate_id = uuid4()

        result = client.list_aggregate_events(aggregate_id)
        self.assertEqual(len(result), 0)

        event_topic = "event topic"
        event_revision = "1"
        event_state = b"123456789"

        client.append_event(
            iter(
                [
                    Event(
                        message_identifier="1sdfsdf",
                        aggregate_identifier=aggregate_id.hex,
                        aggregate_sequence_number=0,
                        aggregate_type="AggregateRoot",
                        timestamp=967868768,
                        payload=SerializedObject(
                            type=event_topic, revision=event_revision, data=event_state
                        ),
                        snapshot=False,
                        meta_data={},
                    )
                ]
            )
        )

        result = client.list_aggregate_events(aggregate_id)
        self.assertEqual(len(result), 1)

        stored_snapshot = result[0]
        self.assertIsInstance(stored_snapshot, Event)
        self.assertEqual(stored_snapshot.message_identifier, "1sdfsdf")
        self.assertEqual(stored_snapshot.aggregate_identifier, aggregate_id.hex)
        self.assertEqual(stored_snapshot.payload.type, event_topic)
        self.assertEqual(stored_snapshot.payload.revision, event_revision)
        self.assertEqual(stored_snapshot.payload.data, event_state)

    def test_append_and_list_snapshot_events(self):
        uri = "localhost:8124"
        client = AxonClient(uri)
        aggregate_id = uuid4()
        result = client.list_snapshot_events(aggregate_id)
        self.assertEqual(result, [])

        client.append_snapshot(
            Event(
                message_identifier="1",
                aggregate_identifier=aggregate_id.hex,
                aggregate_sequence_number=0,
                aggregate_type="AggregateRoot",
                timestamp=0,
                snapshot=True,
                meta_data={},
            )
        )

        result = client.list_snapshot_events(aggregate_id)
        self.assertEqual(len(result), 1)
        stored_snapshot = result[0]
        self.assertIsInstance(stored_snapshot, Event)
        self.assertEqual(stored_snapshot.message_identifier, "1")
        self.assertEqual(stored_snapshot.aggregate_identifier, aggregate_id.hex)
