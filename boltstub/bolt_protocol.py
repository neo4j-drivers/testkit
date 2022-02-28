from .errors import (
    BoltMissingVersionError,
    BoltUnknownMessageError,
    BoltUnknownVersionError,
    ServerExit,
)
from .packstream import Structure
from .simple_jolt import dumps_simple as jolt_dumps
from .util import (
    hex_repr,
    recursive_subclasses,
)


def get_bolt_protocol(version):
    if version is None:
        raise BoltMissingVersionError()
    for sub in recursive_subclasses(BoltProtocol):
        if version == sub.protocol_version or version in sub.version_aliases:
            return sub
    raise BoltUnknownVersionError(
        "unsupported bolt version {}".format(version)
    )


def verify_script_messages(script):
    protocol = get_bolt_protocol(script.context.bolt_version)
    for line in script.client_lines:
        if line.parsed[0] not in protocol.messages["C"].values():
            raise BoltUnknownMessageError(
                "Unsupported client message {} for BOLT version {}. "
                "Must be one of {}".format(
                    line.parsed[0], script.context.bolt_version,
                    list(protocol.messages["C"].values())
                ),
                line
            )
    for line in script.server_lines:
        if line.parsed[0] is None:
            continue  # this server line contains a command, not a message
        if line.parsed[0] not in protocol.messages["S"].values():
            raise BoltUnknownMessageError(
                "Unsupported server message {} for BOLT version {}. "
                "Must be one of {}".format(
                    line.parsed[0], script.context.bolt_version,
                    list(protocol.messages["S"].values())
                ),
                line
            )


class TranslatedStructure(Structure):
    def __init__(self, name, tag, *fields):
        # Verified is false as this class in only used for message structs
        super().__init__(tag, *fields, verified=False)
        self.name = name

    def __repr__(self):
        return "Structure[0x%02X|%s](%s)" % (ord(self.tag), self.name,
                                             ", ".join(map(repr, self.fields)))

    def __str__(self):
        return self.name + " {}".format(" ".join(
            map(jolt_dumps, self.fields_to_jolt_types())
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

    messages = {
        "C": {},
        "S": {},
    }

    @classmethod
    def decode_versions(cls, b):
        assert isinstance(b, (bytes, bytearray))
        assert len(b) == 16
        for spec in (b[i:(i + 4)] for i in range(0, 16, 4)):
            _, range_, minor, major = spec
            for minor in range(minor, minor - range_ - 1, -1):
                yield major, minor

    @classmethod
    def translate_server_line(cls, server_line):
        name, fields = server_line.parsed
        try:
            tag = next(tag_ for tag_, name_ in cls.messages["S"].items()
                       if name == name_)
        except StopIteration as e:
            raise ValueError(
                "Unknown response message type {} in Bolt version {}".format(
                    name, ".".join(map(str, cls.protocol_version))
                )
            ) from e
        return TranslatedStructure(name, tag, *fields)

    @classmethod
    def translate_structure(cls, structure: Structure):
        try:
            return TranslatedStructure(cls.messages["C"][structure.tag],
                                       structure.tag, *structure.fields)
        except KeyError:
            raise ServerExit(
                "Unknown response message type {} in Bolt version {}".format(
                    hex_repr(structure.tag),
                    ".".join(map(str, cls.protocol_version))
                )
            )


class Bolt1Protocol(BoltProtocol):
    protocol_version = (1, 0)
    version_aliases = {(1,), (3, 0), (3, 1), (3, 2), (3, 3)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

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

    @classmethod
    def decode_versions(cls, b):
        # only major version is supported
        # ignore all but last byte
        masked = bytes(0 if i % 4 != 3 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(masked)

    @classmethod
    def get_auto_response(cls, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure("SUCCESS", b"\x70", {
                "server": cls.server_agent,
            })
        else:
            return TranslatedStructure("SUCCESS", b"\x70", {})


class Bolt2Protocol(Bolt1Protocol):
    protocol_version = (2, 0)
    version_aliases = {(2,), (3, 4)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    server_agent = "Neo4j/3.4.0"

    @classmethod
    def get_auto_response(cls, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure("SUCCESS", b"\x70", {
                "server": cls.server_agent,
            })
        else:
            return TranslatedStructure("SUCCESS", b"\x70", {})


class Bolt3Protocol(Bolt2Protocol):
    protocol_version = (3, 0)
    version_aliases = {(3,), (3, 5), (3, 6)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

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

    @classmethod
    def get_auto_response(cls, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure("SUCCESS", b"\x70", {
                "connection_id": "bolt-0",
                "server": cls.server_agent,
            })
        else:
            return TranslatedStructure("SUCCESS", b"\x70", {})


class Bolt4x0Protocol(Bolt3Protocol):
    protocol_version = (4, 0)
    version_aliases = {(4,)}
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

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

    @classmethod
    def decode_versions(cls, b):
        # minor version was introduced
        # ignore first two bytes
        masked = bytes(0 if i % 4 <= 1 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(masked)

    @classmethod
    def get_auto_response(cls, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure("SUCCESS", b"\x70", {
                "connection_id": "bolt-0",
                "server": cls.server_agent,
            })
        else:
            return TranslatedStructure("SUCCESS", b"\x70", {})


class Bolt4x1Protocol(Bolt4x0Protocol):
    protocol_version = (4, 1)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

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

    @classmethod
    def get_auto_response(cls, request: TranslatedStructure):
        if request.tag == b"\x01":
            return TranslatedStructure("SUCCESS", b"\x70", {
                "connection_id": "bolt-0",
                "server": cls.server_agent,
                "routing": None,
            })
        else:
            return TranslatedStructure("SUCCESS", b"\x70", {})


class Bolt4x2Protocol(Bolt4x1Protocol):
    protocol_version = (4, 2)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = {(4, 1)}

    server_agent = "Neo4j/4.2.0"


class Bolt4x3Protocol(Bolt4x2Protocol):
    protocol_version = (4, 3)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

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

    @classmethod
    def decode_versions(cls, b):
        # minor version ranges were introduced
        # ignore first byte
        masked = bytes(0 if i % 4 == 0 else b[i]
                       for i in range(len(b)))
        return BoltProtocol.decode_versions(masked)


class Bolt4x4Protocol(Bolt4x3Protocol):
    protocol_version = (4, 4)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    server_agent = "Neo4j/4.4.0"


class Bolt5x0Protocol(Bolt4x4Protocol):
    protocol_version = (5, 0)
    version_aliases = set()
    # allow the server to negotiate other bolt versions
    equivalent_versions = set()

    server_agent = "Neo4j/5.0.0"
