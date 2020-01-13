# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import axonclient.control_pb2 as control__pb2


class PlatformServiceStub(object):
  """Service describing operations for connecting to the AxonServer platform.

  Clients are expected to use this service on any of the Platform's Admin nodes to obtain connection information of the
  node that it should set up the actual connection with. On that second node, the clients should open an instruction
  stream (see OpenStream), so that AxonServer and the client application can exchange information and instructions.
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.GetPlatformServer = channel.unary_unary(
        '/io.axoniq.axonserver.grpc.control.PlatformService/GetPlatformServer',
        request_serializer=control__pb2.ClientIdentification.SerializeToString,
        response_deserializer=control__pb2.PlatformInfo.FromString,
        )
    self.OpenStream = channel.stream_stream(
        '/io.axoniq.axonserver.grpc.control.PlatformService/OpenStream',
        request_serializer=control__pb2.PlatformInboundInstruction.SerializeToString,
        response_deserializer=control__pb2.PlatformOutboundInstruction.FromString,
        )


class PlatformServiceServicer(object):
  """Service describing operations for connecting to the AxonServer platform.

  Clients are expected to use this service on any of the Platform's Admin nodes to obtain connection information of the
  node that it should set up the actual connection with. On that second node, the clients should open an instruction
  stream (see OpenStream), so that AxonServer and the client application can exchange information and instructions.
  """

  def GetPlatformServer(self, request, context):
    """Obtains connection information for the Server that a Client should use for its connections. 
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def OpenStream(self, request_iterator, context):
    """Opens an instruction stream to the Platform, allowing AxonServer to provide management instructions to the application 
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_PlatformServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'GetPlatformServer': grpc.unary_unary_rpc_method_handler(
          servicer.GetPlatformServer,
          request_deserializer=control__pb2.ClientIdentification.FromString,
          response_serializer=control__pb2.PlatformInfo.SerializeToString,
      ),
      'OpenStream': grpc.stream_stream_rpc_method_handler(
          servicer.OpenStream,
          request_deserializer=control__pb2.PlatformInboundInstruction.FromString,
          response_serializer=control__pb2.PlatformOutboundInstruction.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'io.axoniq.axonserver.grpc.control.PlatformService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
