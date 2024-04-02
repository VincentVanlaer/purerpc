import os
import pytest
import trio.testing
import anyio

import purerpc
from purerpc.test_utils import run_purerpc_service_in_process, run_grpc_service_in_process, grpc_channel, \
    grpc_client_parallelize, purerpc_channel

pytestmark = pytest.mark.anyio


@pytest.fixture()
def purerpc_port(greeter_pb2, greeter_grpc):
    class Servicer(greeter_grpc.GreeterServicer):
        async def SayHello(self, message):
            os._exit(0)

        async def SayHelloToMany(self, messages):
            pass

    with run_purerpc_service_in_process(Servicer().service) as port:
        yield port


async def test_crash_purerpc_client(greeter_pb2, greeter_grpc, purerpc_port):
    with trio.testing.RaisesGroup(anyio.BrokenResourceError, strict=False):
        async with purerpc.insecure_channel("127.0.0.1", purerpc_port) as channel:
            stub = greeter_grpc.GreeterStub(channel)
            await stub.SayHello(greeter_pb2.HelloRequest())
