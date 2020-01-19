import http.client
from unittest import TestCase
from uuid import uuid4

from grpc import StatusCode
from grpc._channel import _InactiveRpcError, _MultiThreadedRendezvous

from axonclient.client import AxonClient, AxonEvent, DEFAULT_LOCAL_AXONSERVER_URI


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
        with self.assertRaises(_MultiThreadedRendezvous):
            client.list_aggregate_events(aggregate_id, 0, False)

    def test_append_and_list_aggregate_events(self):
        # Connect to Axon Server.
        client = AxonClient(DEFAULT_LOCAL_AXONSERVER_URI)
        aggregate_id = uuid4()

        # Check there are zero events in the aggregate sequence.
        result = client.list_aggregate_events(aggregate_id, 0, False)
        self.assertEqual(len(result), 0)

        event_topic = "event topic"
        event_revision = "1"
        event_state = b"123456789"

        # Append a single event.
        client.append_event(
            AxonEvent(
                message_identifier="1sdfsdf",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=0,
                aggregate_type="AggregateRoot",
                timestamp=967868768,
                payload_type=event_topic,
                payload_revision=event_revision,
                payload_data=event_state,
                snapshot=False,
                meta_data={},
            )
        )

        # Check there is one event.
        result = client.list_aggregate_events(aggregate_id, 0, False)
        self.assertEqual(len(result), 1)

        # Fail to append event at same position.
        with self.assertRaises(_InactiveRpcError) as context:
            client.append_event(
                AxonEvent(
                    message_identifier="1sdfsdf",
                    aggregate_identifier=str(aggregate_id),
                    aggregate_sequence_number=0,
                    aggregate_type="AggregateRoot",
                    timestamp=967868768,
                    payload_type=event_topic,
                    payload_revision=event_revision,
                    payload_data=event_state,
                    snapshot=False,
                    meta_data={},
                )
            )
        self.assertEqual(context.exception.args[0].code, StatusCode.OUT_OF_RANGE)
        self.assertIn('Invalid sequence number', context.exception.args[0].details)

        # Check there is still only one event in the aggregate sequence.
        result = client.list_aggregate_events(aggregate_id, 0, False)
        self.assertEqual(len(result), 1)

        stored_event = result[0]
        self.assertIsInstance(stored_event, AxonEvent)
        self.assertEqual(stored_event.message_identifier, "1sdfsdf")
        self.assertEqual(stored_event.aggregate_identifier, str(aggregate_id))
        self.assertEqual(stored_event.aggregate_sequence_number, 0)
        self.assertEqual(stored_event.payload_type, event_topic)
        self.assertEqual(stored_event.payload_revision, event_revision)
        self.assertEqual(stored_event.payload_data, event_state)

        # Append two more events.
        client.append_event([
            AxonEvent(
                message_identifier="1sdfsdf",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=1,
                aggregate_type="AggregateRoot",
                timestamp=967868768,
                payload_type=event_topic,
                payload_revision=event_revision,
                payload_data=event_state,
                snapshot=False,
                meta_data={},
            ),
            AxonEvent(
                message_identifier="1sdfsdf",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=2,
                aggregate_type="AggregateRoot",
                timestamp=967868768,
                payload_type=event_topic,
                payload_revision=event_revision,
                payload_data=event_state,
                snapshot=False,
                meta_data={},
            )
        ])

        # Check there are three events in the aggregate sequence.
        result = client.list_aggregate_events(aggregate_id, 0, False)
        self.assertEqual(len(result), 3)

    def test_list_application_events(self):
        uri = "localhost:8124"
        client = AxonClient(uri)

        # Check there are zero events in the application sequence.
        result = client.list_events(0, 1000, '', '', '', [])
        self.assertEqual(len(result), 0)

        event_topic = "event topic"
        event_revision = "1"
        event_state = b"123456789"
        aggregate_id = uuid4()

        # Append a single event.
        client.append_event(
            AxonEvent(
                message_identifier="1sdfsdf",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=0,
                aggregate_type="AggregateRoot",
                timestamp=967868768,
                payload_type=event_topic,
                payload_revision=event_revision,
                payload_data=event_state,
                snapshot=False,
                meta_data={},
            )
        )

        self.assertEqual(len(result), 1)

    def test_append_and_list_snapshot_events(self):
        uri = "localhost:8124"
        client = AxonClient(uri)
        aggregate_id = uuid4()

        # Check there are no snapshots for this aggregate.
        # result = client.list_snapshot_events(aggregate_id, 0, 1, 1)
        result = client.list_aggregate_events(aggregate_id, 0, allow_snapshots=True)
        self.assertEqual(result, [])

        # Append a snapshot for this aggregate.
        client.append_snapshot(
            AxonEvent(
                message_identifier="1",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=0,
                aggregate_type="AggregateRoot",
                timestamp=0,
                payload_type='',
                payload_revision='1',
                payload_data=b'',
                snapshot=True,
                meta_data={},
            )
        )

        # Check there is one snapshot for this aggregate.
        # result = client.list_snapshot_events(aggregate_id, 0, 1, 1)
        result = client.list_aggregate_events(aggregate_id, 0, allow_snapshots=True)
        self.assertEqual(len(result), 1)

        stored_snapshot = result[0]
        self.assertIsInstance(stored_snapshot, AxonEvent)
        self.assertEqual(stored_snapshot.aggregate_sequence_number, 0)
        self.assertEqual(stored_snapshot.message_identifier, "1")
        self.assertEqual(stored_snapshot.aggregate_identifier, str(aggregate_id))

        # Fail to append snapshot at same position.
        client.append_snapshot(
            AxonEvent(
                message_identifier="2",
                aggregate_identifier=str(aggregate_id),
                aggregate_sequence_number=1,
                aggregate_type="AggregateRoot",
                timestamp=1,
                payload_type='',
                payload_revision='1',
                payload_data=b'',
                snapshot=True,
                meta_data={},
            )
        )

        result = client.list_aggregate_events(aggregate_id, 0, allow_snapshots=True)
        # result = client.list_snapshot_events(aggregate_id, 1, 2, 1)
        self.assertEqual(len(result), 1)
        stored_snapshot = result[0]
        self.assertIsInstance(stored_snapshot, AxonEvent)
        self.assertEqual(stored_snapshot.aggregate_sequence_number, 1)
        self.assertEqual(stored_snapshot.message_identifier, "2")
        self.assertEqual(stored_snapshot.aggregate_identifier, str(aggregate_id))
