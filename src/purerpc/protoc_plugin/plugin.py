import sys
import itertools

from google.protobuf.compiler.plugin_pb2 import CodeGeneratorRequest
from google.protobuf.compiler.plugin_pb2 import CodeGeneratorResponse
from google.protobuf import descriptor_pb2
from purerpc import Cardinality


def generate_import_statement(proto_name):
    module_path = proto_name[:-len(".proto")].replace("-", "_").replace("/", ".") + "_pb2"
    alias = get_python_module_alias(proto_name)
    if "." in module_path:
        # produces import statements in line with grpcio so that tools for 
        # postprocessing import statements work with purerpc as well
        # example: `from foo.bar import zap_pb2 as foo_dot_bar_dot_zap__pb2`
        parent_modules, sub_module = module_path.rsplit(".", 1)
        return "from " + parent_modules + " import " + sub_module + " as " + alias
    else:
        return "import " + module_path + " as " + alias


def get_python_module_alias(proto_name):
    package_name = proto_name[:-len(".proto")]
    return package_name.replace("/", "_dot_").replace("-", "_") + "__pb2"


def simple_type(type_):
    simple_type = type_.split(".")[-1]
    return simple_type


def get_python_type(proto_name, proto_type):
    if proto_type.startswith("."):
        return get_python_module_alias(proto_name) + "." + simple_type(proto_type)
    else:
        return proto_type


def generate_single_proto(proto_file: descriptor_pb2.FileDescriptorProto,
                          proto_for_entity):
    lines = [
        "# mypy: ignore-errors",
        "from __future__ import annotations",
        "from typing import Callable, AsyncIterator, Coroutine, Any",
        "from contextlib import AbstractAsyncContextManager",
        "import purerpc"
    ]

    lines.append(generate_import_statement(proto_file.name))
    for dep_module in proto_file.dependency:
        lines.append(generate_import_statement(dep_module))
    for service in proto_file.service:
        if proto_file.package:
            fully_qualified_service_name = proto_file.package + "." + service.name
        else:
            fully_qualified_service_name = service.name

        # Servicer
        lines.append(f"\n\nclass {service.name}Servicer(purerpc.Servicer):")

        for method in service.method:
            plural_suffix = "s" if method.client_streaming else ""
            input_proto = proto_for_entity[method.input_type]
            output_proto = proto_for_entity[method.output_type]
            input_type = get_python_type(input_proto, method.input_type)
            output_type = get_python_type(output_proto, method.output_type)

            if method.server_streaming:
                output_type = f"AsyncIterator[{output_type}]"

            if method.client_streaming:
                input_type = f"AsyncIterator[{input_type}]"

            lines.append(f"    async def {method.name}(self, input_message{plural_suffix}: {input_type}) -> {output_type}:\n"
                         f"        raise NotImplementedError()")

            # ensure the type gets inferred correctly
            if method.server_streaming:
                lines.append("        yield\n")
            else:
                lines.append("")

        lines.append(f"    @property\n"
                     f"    def service(self) -> purerpc.Service:\n"
                     f"        service_obj = purerpc.Service(\n"
                     f"            \"{fully_qualified_service_name}\"\n"
                     f"        )")

        for method in service.method:
            input_proto = proto_for_entity[method.input_type]
            output_proto = proto_for_entity[method.output_type]
            input_type = get_python_type(input_proto, method.input_type)
            output_type = get_python_type(output_proto, method.output_type)
            cardinality = Cardinality.get_cardinality_for(request_stream=method.client_streaming,
                                                          response_stream=method.server_streaming)

            lines.append(f"        service_obj.add_method(\n"
                         f"            \"{method.name}\",\n"
                         f"            self.{method.name},\n"
                         f"            purerpc.RPCSignature(\n"
                         f"                purerpc.{cardinality},\n"
                         f"                {input_type},\n"
                         f"                {output_type},\n"
                         f"            )\n"
                         f"        )")

        lines.append("        return service_obj\n\n")

        # Stub
        lines.append(f"class {service.name}Stub:\n"
                     f"    def __init__(self, channel: purerpc.client._Channel) -> None:\n"
                     f"        self._client = purerpc.Client(\n"
                     f"            \"{fully_qualified_service_name}\",\n"
                     f"            channel\n"
                     f"        )")

        for method in service.method:
            input_proto = proto_for_entity[method.input_type]
            output_proto = proto_for_entity[method.output_type]
            cardinality = Cardinality.get_cardinality_for(request_stream=method.client_streaming,
                                                          response_stream=method.server_streaming)
            input_type = get_python_type(input_proto, method.input_type)
            output_type = get_python_type(output_proto, method.output_type)

            if method.client_streaming:
                arg_type = f"AsyncIterator[{input_type}]"
            else:
                arg_type = input_type

            if method.server_streaming:
                if method.client_streaming:
                    return_type = f"AbstractAsyncContextManager[AsyncIterator[{output_type}]]"
                else:
                    return_type = f"AsyncIterator[{output_type}]"
            else:
                return_type = f"Coroutine[Any, Any, {output_type}]"


            lines.append(f"        self.{method.name}: Callable[[{arg_type}], {return_type}] = self._client.get_method_stub(\n"
                         f"            \"{method.name}\",\n"
                         f"            purerpc.RPCSignature(\n"
                         f"                purerpc.{cardinality},\n"
                         f"                {input_type},\n"
                         f"                {output_type},\n"
                         f"            )\n"
                         f"        )")

    return "\n".join(lines)


def main():
    request = CodeGeneratorRequest.FromString(sys.stdin.buffer.read())

    files_to_generate = set(request.file_to_generate)

    response = CodeGeneratorResponse()
    proto_for_entity = dict()
    for proto_file in request.proto_file:
        package_name = proto_file.package
        for named_entity in itertools.chain(proto_file.message_type, proto_file.enum_type,
                                            proto_file.service, proto_file.extension):
            if package_name:
                fully_qualified_name = ".".join(["", package_name, named_entity.name])
            else:
                fully_qualified_name = "." + named_entity.name
            proto_for_entity[fully_qualified_name] = proto_file.name
    for proto_file in request.proto_file:
        if proto_file.name in files_to_generate:
            out = response.file.add()
            out.name = proto_file.name.replace('-', "_").replace('.proto', "_grpc.py")
            out.content = generate_single_proto(proto_file, proto_for_entity)
    sys.stdout.buffer.write(response.SerializeToString())
