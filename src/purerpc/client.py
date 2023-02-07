import functools
import socket
from contextlib import AsyncExitStack

import anyio

from purerpc.grpc_proto import GRPCProtoSocket
from purerpc.grpclib.config import GRPCConfiguration
from purerpc.rpc import RPCSignature, Cardinality
from purerpc.utils import is_darwin, is_windows
from purerpc.wrappers import ClientStubUnaryUnary, ClientStubStreamStream, ClientStubUnaryStream, \
    ClientStubStreamUnary


class _Channel(AsyncExitStack):
    SCHEME = "http"

    def __init__(self, host, port, ssl_context=None):
        super().__init__()
        self._host = host
        self._port = port
        self._ssl_context = ssl_context
        self._grpc_socket = None

    async def __aenter__(self):
        await super().__aenter__()  # Does nothing
        socket = await anyio.connect_tcp(self._host, self._port,
                                         ssl_context=self._ssl_context,
                                         tls=self._ssl_context is not None,
                                         tls_standard_compatible=False)
        self._set_socket_options(socket)
        config = GRPCConfiguration(client_side=True)
        self._grpc_socket = await self.enter_async_context(GRPCProtoSocket(config, socket))
        return self

    @staticmethod
    def _set_socket_options(stream: anyio.abc.SocketStream):
        sock = stream.extra(anyio.abc.SocketAttribute.raw_socket)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 300)
        elif is_darwin():
            # Darwin specific option
            TCP_KEEPALIVE = 16
            sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, 300)
        if not is_windows():
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def authority(self):
        return f"{self._host}:{self._port}"


class _UnixChannel(AsyncExitStack):
    SCHEME = "unix"

    def __init__(self, path):
        super().__init__()
        self._path = path
        self._grpc_socket = None

    async def __aenter__(self):
        await super().__aenter__()  # Does nothing
        socket = await anyio.connect_unix(self._path)
        config = GRPCConfiguration(client_side=True)
        self._grpc_socket = await self.enter_async_context(GRPCProtoSocket(config, socket))
        return self

    def authority(self):
        return self._path

def insecure_channel(host, port):
    return _Channel(host, port)

def secure_channel(host, port, ssl_context):
    return _Channel(host, port, ssl_context)

def unix_channel(path):
    return _UnixChannel(path)

class Client:
    def __init__(self, service_name: str, channel: _Channel):
        self.service_name = service_name
        self.channel = channel

    async def rpc(self, method_name: str, request_type, response_type, metadata=None):
        message_type = request_type.DESCRIPTOR.full_name
        if metadata is None:
            metadata = ()
        stream = await self.channel._grpc_socket.start_request(self.channel.SCHEME, self.service_name,
                                                               method_name, message_type,
                                                               self.channel.authority(),
                                                               custom_metadata=metadata)
        stream.expect_message_type(response_type)
        return stream

    def get_method_stub(self, method_name: str, signature: RPCSignature):
        stream_fn = functools.partial(self.rpc, method_name, signature.request_type,
                                      signature.response_type)
        if signature.cardinality == Cardinality.STREAM_STREAM:
            return ClientStubStreamStream(stream_fn)
        elif signature.cardinality == Cardinality.UNARY_STREAM:
            return ClientStubUnaryStream(stream_fn)
        elif signature.cardinality == Cardinality.STREAM_UNARY:
            return ClientStubStreamUnary(stream_fn)
        else:
            return ClientStubUnaryUnary(stream_fn)
