from typing import Iterable, Optional, Union
from uuid import UUID

import grpc
from grpc._channel import _MultiThreadedRendezvous

from axonclient.common_pb2 import SerializedObject
from axonclient.event_pb2 import Event, GetAggregateEventsRequest, PayloadDescription
from axonclient.event_pb2_grpc import EventStoreStub


class AxonEvent(object):
    def __init__(
        self,
        message_identifier: str,
        aggregate_identifier: str,
        aggregate_sequence_number: int,
        aggregate_type: str,
        timestamp: int,
        payload_type: str,
        payload_revision: str,
        payload_data: bytes,
        snapshot: bool,
        meta_data: dict,
    ):
        self.message_identifier = message_identifier
        self.aggregate_identifier = aggregate_identifier
        self.aggregate_sequence_number = aggregate_sequence_number
        self.aggregate_type = aggregate_type
        self.timestamp = timestamp
        self.payload_type = payload_type
        self.payload_revision = payload_revision
        self.payload_data = payload_data
        self.snapshot = snapshot
        self.meta_data = meta_data

    def to_grpc(self):
        return Event(
            message_identifier=self.message_identifier,
            aggregate_identifier=self.aggregate_identifier,
            aggregate_sequence_number=self.aggregate_sequence_number,
            aggregate_type=self.aggregate_type,
            timestamp=self.timestamp,
            payload=SerializedObject(
                type=self.payload_type,
                revision=self.payload_revision,
                data=self.payload_data,
            ),
            snapshot=self.snapshot,
            meta_data=self.meta_data,
        )

    @classmethod
    def from_grpc(cls, event: Event):
        return cls(
            message_identifier=event.message_identifier,
            aggregate_identifier=event.aggregate_identifier,
            aggregate_sequence_number=event.aggregate_sequence_number,
            aggregate_type=event.aggregate_type,
            timestamp=event.timestamp,
            payload_type=event.payload.type,
            payload_revision=event.payload.revision,
            payload_data=event.payload.data,
            snapshot=event.snapshot,
            meta_data=event.meta_data,
        )


class AxonClient:
    def __init__(self, uri):
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.event_store_stub = EventStoreStub(self.channel)

    def list_aggregate_events(self, aggregate_id: UUID, initial_sequence: int, allow_snapshots: bool):
        return list(self.iter_aggregate_events(aggregate_id, initial_sequence, allow_snapshots))

    def iter_aggregate_events(self, aggregate_id: UUID, initial_sequence: int, allow_snapshots: bool):
        request = GetAggregateEventsRequest(
            aggregate_id=aggregate_id.hex,
            initial_sequence=initial_sequence,
            allow_snapshots=allow_snapshots,
        )
        response = self.event_store_stub.ListAggregateEvents(request)
        for event in response:
            yield AxonEvent.from_grpc(event)

    def append_event(self, events: Union[AxonEvent, Iterable[AxonEvent]]):
        if isinstance(events, AxonEvent):
            events = [events]
        confirmation = self.event_store_stub.AppendEvent(
            (event.to_grpc() for event in events)
        )
        assert confirmation.success, "Operation failed"

    def list_snapshot_events(self, aggregate_id, initial_sequence, max_sequence, max_reults):
        return list(self.iter_snapshot_events(aggregate_id, initial_sequence, max_sequence, max_reults))

    def iter_snapshot_events(self, aggregate_id, initial_sequence, max_sequence, max_reults):
        request = GetAggregateEventsRequest(aggregate_id=aggregate_id.hex)
        response = self.event_store_stub.ListAggregateSnapshots(request)
        for event in response:
            yield AxonEvent.from_grpc(event)

    def append_snapshot(self, event: AxonEvent):
        confirmation = self.event_store_stub.AppendSnapshot(event.to_grpc())
        assert confirmation.success, "Operation failed"
