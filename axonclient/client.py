from uuid import UUID

import grpc
from grpc import StatusCode
from grpc._channel import _Rendezvous

from axonclient.event_pb2 import GetAggregateEventsRequest
from axonclient.event_pb2_grpc import EventStoreStub


class AxonClientError(Exception):
    pass


class AxonClient():
    def __init__(self, uri):
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.event_store_stub = EventStoreStub(self.channel)

    def list_aggregate_events(self, aggregate_id: UUID):
        request = GetAggregateEventsRequest(aggregate_id=aggregate_id.hex)
        response = self.event_store_stub.ListAggregateEvents(request)
        try:
            return list(response)
        except Exception as e:
            raise AxonClientError(e)

    def append_event(self, event):
        self.event_store_stub.AppendEvent(event)
