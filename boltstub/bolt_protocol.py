# Copyright (c) "Neo4j,"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from threading import Lock

from .errors import (
    BoltFeatureError,
    BoltMissingVersionError,
    BoltUnknownMessageError,
    BoltUnknownVersionError,
    ServerExit,
)
from .packstream import Structure
from .simple_jolt import v1 as jolt_v1
from .simple_jolt import v2 as jolt_v2
from .util import (
    hex_repr,
    recursive_subclasses,
)

jolt_package = {
    1: jolt_v1,
    2: jolt_v2,
}

auto_bolt_id = 0
auto_bolt_id_lock = Lock()


def next_auto_bolt_id():
    global auto_bolt_id
    with auto_bolt_id_lock:
        auto_bolt_id += 1
        return f"bolt-{auto_bolt_id}"


def get_bolt_protocol(version, features=None):
    if version is None:
        raise BoltMissingVersionError()
    for sub in recursive_subclasses(BoltProtocol):
        if (
            version == sub.protocol_version
            or version in sub.version_aliases
        ):
            return sub(features)
    raise BoltUnknownVersionError(
        "unsupported bolt version {}".format(version)
    )


def verify_script_messages(script):
    protocol = get_bolt_protocol(
        script.context.bolt_version,
        script.context.bolt_features,
    )
    for line in script.client_lines:
        # will raise an exception if the message is unknown or the body
        # cannot be decoded
        protocol.translate_client_line(line)
    for line in script.server_lines:
        if line.is_command:
            continue  # this server line contains a command, not a message
        # will raise an exception if the message is unknown
        protocol.translate_server_line(line)


class TranslatedStructure(Structure):
    def __init__(self, name, tag, *fields, packstream_version):
        # Verified is false as this class in only used for message structs
        super().__init__(tag, *fields, packstream_version=packstream_version,
                         verified=False)
        self.name = name

    def __repr__(self):
        return "Structure[0x%02X|%s](%s)" % (ord(self.tag), self.name,
                                             ", ".join(map(repr, self.fields)))

    def __str__(self):
        return self.name + " {}".format(" ".join(
            map(jolt_package[self.packstream_version].dumps_simple,
                self.fields_to_jolt_types())
        ))

    def __eq__(self, other):
        try:
            return (self.tag == other.tag
                    and self.fields == other.fields
                    and self.name == other.name)
        except AttributeError:
            return False


class BoltProtocol:
    protocol_version = None
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = None
    handshake_version = None
    features = b"\x00"

    messages = {
        "C": {},
        "S": {},
    }

    def __init__(self, features=None):
        if features is not None:
            raise BoltFeatureError(
                f"{self.__class__.__name__} does not support bolt feature "
                f"flags"
            )

    def decode_versions(self, b):
        assert isinstance(b, (bytes, bytearray))
        assert len(b) == 16
        for spec in (b[i:(i + 4)] for i in range(0, 16, 4)):
            _, range_, spec_minor, major = spec
            for minor in range(spec_minor, spec_minor - range_ - 1, -1):
                yield major, minor

    def translate_client_line(self, client_line):
        if not client_line.jolt_parsed:
            client_line.parse_jolt(self.get_jolt_package())
        name, fields = client_line.jolt_parsed
        try:
            tag = next(tag_ for tag_, name_ in self.messages["C"].items()
                       if name == name_)
        except StopIteration:
            raise BoltUnknownMessageError(
                "Unsupported client message {} for BOLT version {}. "
                "Must be one of {}".format(
                    name, self.protocol_version,
                    list(self.messages["C"].values())
                ),
                client_line
            )
        return TranslatedStructure(
            name, tag, *fields, packstream_version=self.packstream_version
        )

    def translate_server_line(self, server_line):
        if not server_line.jolt_parsed:
            server_line.parse_jolt(self.get_jolt_package())
        name, fields = server_line.jolt_parsed
        try:
            tag = next(tag_ for tag_, name_ in self.messages["S"].items()
                       if name == name_)
        except StopIteration:
            raise BoltUnknownMessageError(
                "Unsupported server message {} for BOLT version {}. "
                "Must be one of {}".format(
                    name, self.protocol_version,
                    list(self.messages["S"].values())
                ),
                server_line
            )
        return TranslatedStructure(
            name, tag, *fields, packstream_version=self.packstream_version
        )

    def translate_structure(self, structure: Structure):
        try:
            return TranslatedStructure(
                self.messages["C"][structure.tag], structure.tag,
                *structure.fields, packstream_version=self.packstream_version
            )
        except KeyError:
            raise ServerExit(
                "Unknown response message type {} in Bolt version {}".format(
                    hex_repr(structure.tag),
                    ".".join(map(str, self.protocol_version))
                )
            )

    def get_jolt_package(self):
        return jolt_package[self.packstream_version]


class Bolt1Protocol(BoltProtocol):

    protocol_version = (1, 0)
    version_aliases = {(1,), (3, 0), (3, 1), (3, 2), (3, 3)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    messages = {
        "C": {
            b"\x01": "INIT",
            b"\x0E": "ACK_FAILURE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x2F": "DISCARD_ALL",
            b"\x3F": "PULL_ALL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/3.3.0"

    def decode_versions(self, b):
        # only major version is supported
        # ignore all but last byte
        masked = bytes(0 if i % 4 != 3 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(self, masked)

    def get_auto_response(self, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure(
                "SUCCESS", b"\x70", {"server": self.server_agent},
                packstream_version=self.packstream_version
            )
        else:
            return TranslatedStructure(
                "SUCCESS", b"\x70", {},
                packstream_version=self.packstream_version
            )


class Bolt2Protocol(Bolt1Protocol):

    protocol_version = (2, 0)
    version_aliases = {(2,), (3, 4)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    server_agent = "Neo4j/3.4.0"


class Bolt3Protocol(Bolt2Protocol):

    protocol_version = (3, 0)
    version_aliases = {(3,), (3, 5), (3, 6)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD_ALL",
            b"\x3F": "PULL_ALL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/3.5.0"

    def get_auto_response(self, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure(
                "SUCCESS", b"\x70",
                {
                    "connection_id": next_auto_bolt_id(),
                    "server": self.server_agent
                },
                packstream_version=self.packstream_version
            )
        return super().get_auto_response(request)


class Bolt4x0Protocol(Bolt3Protocol):

    protocol_version = (4, 0)
    version_aliases = {(4,)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD",
            b"\x3F": "PULL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/4.0.0"

    def decode_versions(self, b):
        # minor version was introduced
        # ignore first two bytes
        masked = bytes(0 if i % 4 <= 1 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(self, masked)

    def get_auto_response(self, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure(
                "SUCCESS", b"\x70",
                {
                    "connection_id": next_auto_bolt_id(),
                    "server": self.server_agent
                },
                packstream_version=self.packstream_version
            )
        return super().get_auto_response(request)


class Bolt4x1Protocol(Bolt4x0Protocol):

    protocol_version = (4, 1)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD",
            b"\x3F": "PULL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/4.1.0"

    def get_auto_response(self, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure(
                "SUCCESS", b"\x70",
                {
                    "connection_id": next_auto_bolt_id(),
                    "server": self.server_agent,
                    "routing": None,
                },
                packstream_version=self.packstream_version
            )
        return super().get_auto_response(request)


class Bolt4x2Protocol(Bolt4x1Protocol):

    protocol_version = (4, 2)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = {(4, 1)}

    packstream_version = 1
    handshake_version = 1

    server_agent = "Neo4j/4.2.0"

    def decode_versions(self, b):
        # Minor version ranges were introduced.
        # Officially from bolt 4.3 onwards, but they were ported back to some
        # new 4.2.x server versions.
        # ignore first byte
        masked = bytes(0 if i % 4 == 0 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(self, masked)


class Bolt4x3Protocol(Bolt4x2Protocol):

    protocol_version = (4, 3)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD",
            b"\x3F": "PULL",
            b"\x66": "ROUTE"
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/4.3.0"


class Bolt4x4Protocol(Bolt4x3Protocol):

    protocol_version = (4, 4)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 1
    handshake_version = 1

    server_agent = "Neo4j/4.4.0"


class Bolt5x0Protocol(Bolt4x4Protocol):

    protocol_version = (5, 0)
    version_aliases = {(5,)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    server_agent = "Neo4j/5.0.0"


class Bolt5x1Protocol(Bolt5x0Protocol):

    protocol_version = (5, 1)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    messages = {
        "C": {
            **Bolt5x0Protocol.messages["C"],
            b"\x6A": "LOGON",
            b"\x6B": "LOGOFF",
        },
        "S": Bolt5x0Protocol.messages["S"],
    }

    server_agent = "Neo4j/5.5.0"


class Bolt5x2Protocol(Bolt5x1Protocol):
    protocol_version = (5, 2)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    server_agent = "Neo4j/5.7.0"


class Bolt5x3Protocol(Bolt5x2Protocol):
    protocol_version = (5, 3)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    server_agent = "Neo4j/5.9.0"


class Bolt5x4Protocol(Bolt5x3Protocol):
    protocol_version = (5, 4)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    messages = {
        "C": {
            **Bolt5x3Protocol.messages["C"],
            b"\x54": "TELEMETRY",
        },
        "S": Bolt5x3Protocol.messages["S"],
    }

    server_agent = "Neo4j/5.13.0"


class Bolt5x5Protocol(Bolt5x4Protocol):
    protocol_version = (5, 5)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    server_agent = "Neo4j/5.21.0"


class Bolt5x6Protocol(Bolt5x5Protocol):
    protocol_version = (5, 6)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 1

    server_agent = "Neo4j/5.23.0"


class Bolt5x7Protocol(Bolt5x6Protocol):
    protocol_version = (5, 7)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    packstream_version = 2
    handshake_version = 2

    server_agent = "Neo4j/5.24.0"

    def __init__(self, features=None):
        super().__init__(features=None)
        if features is None:
            features = b"\x00"
        if not self._is_varint(features):
            raise BoltFeatureError("Feature flags must be a varint encoded")
        self.features = features

    @staticmethod
    def _is_varint(b):
        return all(b & 0x80 for b in b[:-1]) and not b[-1] & 0x80

    def get_auto_response(self, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure(
                "SUCCESS", b"\x70",
                {
                    "connection_id": next_auto_bolt_id(),
                    "server": self.server_agent,
                    "routing": None,
                    "protocol_version": ".".join(
                        map(str, self.protocol_version)
                    ),
                },
                packstream_version=self.packstream_version
            )
        return super().get_auto_response(request)
