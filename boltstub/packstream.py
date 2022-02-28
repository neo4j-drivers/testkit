#!/usr/bin/env python

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from codecs import decode
from io import BytesIO
from struct import pack as struct_pack
from struct import unpack as struct_unpack

from .bolt_protocol import BoltProtocol
from .simple_jolt import types as jolt_types

PACKED_UINT_8 = [struct_pack(">B", value) for value in range(0x100)]
PACKED_UINT_16 = [struct_pack(">H", value) for value in range(0x10000)]

UNPACKED_UINT_8 = {bytes(bytearray([x])): x for x in range(0x100)}
UNPACKED_UINT_16 = {struct_pack(">H", x): x for x in range(0x10000)}

UNPACKED_MARKERS = {b"\xC0": None, b"\xC2": False, b"\xC3": True}
UNPACKED_MARKERS.update({bytes(bytearray([z])): z for z in range(0x00, 0x80)})
UNPACKED_MARKERS.update({bytes(bytearray([z + 256])): z
                         for z in range(-0x10, 0x00)})


INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63


EndOfStream = object()


class StructTag:
    node = b"\x4E"
    relationship = b"\x52"
    unbound_relationship = b"\x72"
    path = b"\x50"
    date = b"\x44"
    time = b"\x54"
    local_time = b"\x74"
    date_time = b"\x46"
    date_time_zone_id = b"\x66"
    local_date_time = b"\x64"
    duration = b"\x45"
    point_2d = b"\x58"
    point_3d = b"\x59"




class Structure:

    def __init__(self, tag, *fields, verified=True,
                 bolt_version=BoltProtocol()):
        self.tag = tag
        self.fields = list(fields)
        self._verified = verified
        self.validate_v3 = bolt_version.protocol_version[0] < 5

        if verified:
            self._verify_fields()

    @property
    def verified(self):
        return self._verified

    def _verify_fields(self):
        tag, fields = self.tag, self.fields

        def validate_validations(validations, type_name):
            for validation in validations:
                if not validation(fields):
                    raise ValueError("Invalid %dsn struct received %r" %
                                     type_name, self)

        def verify_node_3_up():
            validations = [
                lambda f: len(f) == 3,
                lambda f: isinstance(f[0], int),
                lambda f: isinstance(f[1], list),
                lambda f: all(isinstance(label, str) for label in f[1]),
                lambda f: isinstance(f[2], dict),
                lambda f: all(isinstance(k, str) for k in f[2].keys())
            ]
            validate_validations(validations, "V3_node")

        def verify_node_5_up():
            validations = [
                lambda f: len(f) == 4,
                lambda f: isinstance(f[0], int) or f[0] is None,
                lambda f: isinstance(f[1], list),
                lambda f: all(isinstance(label, str) for label in f[1]),
                lambda f: isinstance(f[2], dict),
                lambda f: all(isinstance(k, str) for k in f[2].keys()),
                lambda f: isinstance(f[3], str)
            ]
            validate_validations(validations, "V5_node")

        def verify_unbound_relationship_3_up():
            validations = [
                lambda f: len(f) == 3,
                lambda f: isinstance(f[0], int),
                lambda f: isinstance(f[1], str),
                lambda f: isinstance(f[2], dict),
                lambda f: all(isinstance(k, str) for k in f[2].keys())
            ]
            validate_validations(validations, "V3_UnboundRelationship")

        def verify_unbound_relationship_5_up():
            validations = [
                lambda f: len(f) == 4,
                lambda f: isinstance(f[0], int) or f[0] is None,
                lambda f: isinstance(f[1], str),
                lambda f: isinstance(f[2], dict),
                lambda f: all(isinstance(k, str) for k in f[2].keys()),
                lambda f: isinstance(f[3], str)
            ]
            validate_validations(validations, "V5_UnboundRelationship")

        def verify_relationship_3_up():
            validations = [
                lambda f: len(f) == 5,
                lambda f: isinstance(f[0], int),
                lambda f: isinstance(f[1], int),
                lambda f: isinstance(f[2], int),
                lambda f: isinstance(f[3], str),
                lambda f: isinstance(f[4], dict),
                lambda f: all(isinstance(k, str) for k in f[4].keys())
            ]
            validate_validations(validations, "V3_Relationship")

        def verify_relationship_5_up():
            validations = [
                lambda f: len(f) == 8,
                lambda f: (isinstance(f[0], int) or f[0] is None),
                lambda f: (isinstance(f[1], int) or f[1] is None),
                lambda f: (isinstance(f[2], int) or f[2] is None),
                lambda f: isinstance(f[3], str),
                lambda f: isinstance(f[4], dict),
                lambda f: all(isinstance(k, str) for k in f[4].keys()),
                lambda f: isinstance(f[5], str),
                lambda f: isinstance(f[6], str),
                lambda f: isinstance(f[7], str),
            ]
            validate_validations(validations, "V5_Relationship")

        def verify_path():

            if (len(fields) != 3
                    or not isinstance(fields[0], list)
                    or not all(isinstance(n, Structure)
                               and n.tag == StructTag.node
                               and n.fields[0] in fields[2]  # id is used
                               for n in fields[0])
                    or not isinstance(fields[1], list)
                    or not all(isinstance(rel, Structure)
                               and rel.tag == StructTag.unbound_relationship
                               and rel.fields[0] in fields[2]  # id is used
                               for rel in fields[1])
                    or not isinstance(fields[2], list)
                    or not all(isinstance(id_, int)
                               # id exists in nodes or relationships
                               and id_ in {s.fields[0]
                                           for s in fields[0] + fields[1]}
                               for id_ in fields[2])):
                raise ValueError(
                    "Invalid Path struct received %r" % self
                )

        def build_generic_verifier(types, name):
            def verify():
                if (len(fields) != len(types)
                        or not all(isinstance(f, t)
                                   for f, t in zip(fields, types))):
                    raise ValueError(
                        "Invalid %s struct received %r" % (name, self)
                    )

            return verify

        field_validator = {
            StructTag.node:
                verify_node_3_up if self.validate_v3
                else verify_node_5_up,
            StructTag.relationship:
                verify_relationship_3_up if self.validate_v3
                else verify_relationship_5_up,
            StructTag.unbound_relationship:
                verify_unbound_relationship_3_up if self.validate_v3
                else verify_unbound_relationship_5_up,
            StructTag.path:
                verify_path,
            StructTag.date:
                build_generic_verifier((int,), "Date"),
            StructTag.time:
                build_generic_verifier((int, int), "Time"),
            StructTag.local_time:
                build_generic_verifier((int,), "LocalTime"),
            StructTag.date_time:
                build_generic_verifier((int, int, int,), "DateTime"),
            StructTag.date_time_zone_id:
                build_generic_verifier((int, int, str), "DateTimeZoneId"),
            StructTag.local_date_time:
                build_generic_verifier((int, int), "LocalDateTime"),
            StructTag.duration:
                build_generic_verifier((int, int, int, int), "Duration"),
            StructTag.point_2d:
                build_generic_verifier((int, float, float), "Point2D"),
            StructTag.point_3d:
                build_generic_verifier((int, float, float, float), "Point3D"),
        }

        if tag in field_validator:
            field_validator[tag]()

    def __repr__(self):
        return "Structure[0x%02X](%s)" % (ord(self.tag),
                                          ", ".join(map(repr, self.fields)))

    def __eq__(self, other):
        try:
            if self.tag == StructTag.path:
                # path struct => order of nodes and rels is irrelevant
                return (other.tag == StructTag.path
                        and len(other.fields) == 3
                        and sorted(self.fields[0]) == sorted(other.fields[0])
                        and sorted(self.fields[1]) == sorted(other.fields[1])
                        and self.fields[2] == other.fields[2])
            return self.tag == other.tag and self.fields == other.fields
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, Structure):
            return NotImplemented
        return (self.tag, *self.fields) < (other.tag, *other.fields)

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, key):
        return self.fields[key]

    def __setitem__(self, key, value):
        self.fields[key] = value
        if self._verified:
            self._verify_fields()

    def match_jolt_wildcard(self, wildcard: jolt_types.JoltWildcard):
        for t in wildcard.types:
            if issubclass(t, jolt_types.JoltDate):
                if self.tag == StructTag.date:
                    return True
            elif issubclass(t, jolt_types.JoltTime):
                if self.tag == StructTag.time:
                    return True
            elif issubclass(t, jolt_types.JoltLocalTime):
                if self.tag == StructTag.local_time:
                    return True
            elif issubclass(t, jolt_types.JoltDateTime):
                if self.tag == StructTag.date_time:
                    return True
            elif issubclass(t, jolt_types.JoltLocalDateTime):
                if self.tag == StructTag.local_date_time:
                    return True
            elif issubclass(t, jolt_types.JoltDuration):
                if self.tag == StructTag.duration:
                    return True
            elif issubclass(t, jolt_types.JoltPoint):
                if self.tag in (StructTag.point_2d, StructTag.point_3d):
                    return True
            elif issubclass(t, jolt_types.JoltNode):
                if self.tag == StructTag.node:
                    return True
            elif issubclass(t, jolt_types.JoltRelationship):
                if self.tag == StructTag.relationship:
                    return True
            elif issubclass(t, jolt_types.JoltPath):
                if self.tag == StructTag.path:
                    return True

    @classmethod
    def from_jolt_type(cls, jolt: jolt_types.JoltType):
        if isinstance(jolt, jolt_types.JoltDate):
            return cls(StructTag.date, jolt.days)
        if isinstance(jolt, jolt_types.JoltTime):
            return cls(StructTag.time, jolt.nanoseconds, jolt.utc_offset)
        if isinstance(jolt, jolt_types.JoltLocalTime):
            return cls(StructTag.local_time, jolt.nanoseconds)
        if isinstance(jolt, jolt_types.JoltDateTime):
            return cls(StructTag.date_time, *jolt.seconds_nanoseconds,
                       jolt.time.utc_offset)
        if isinstance(jolt, jolt_types.JoltLocalDateTime):
            return cls(StructTag.local_date_time, *jolt.seconds_nanoseconds)
        if isinstance(jolt, jolt_types.JoltDuration):
            return cls(StructTag.duration, jolt.months, jolt.days,
                       jolt.seconds, jolt.nanoseconds)
        if isinstance(jolt, jolt_types.JoltPoint):
            if jolt.z is None:  # 2D
                return cls(StructTag.point_2d, jolt.srid, jolt.x, jolt.y)
            else:
                return cls(StructTag.point_3d, jolt.srid, jolt.x, jolt.y,
                           jolt.z)
        if isinstance(jolt, jolt_types.JoltNode):
            return cls(StructTag.node, jolt.id, jolt.labels, jolt.properties)
        if isinstance(jolt, jolt_types.JoltRelationship):
            return cls(StructTag.relationship, jolt.id, jolt.start_node_id,
                       jolt.end_node_id, jolt.rel_type, jolt.properties)
        if isinstance(jolt, jolt_types.JoltPath):
            # Node structs
            nodes = []
            for node in jolt.path[::2]:
                node = cls(StructTag.node, node.id, node.labels,
                           node.properties)
                if node not in nodes:
                    nodes.append(node)
            # UnboundRelationship structs
            rels = []
            for rel in jolt.path[1::2]:
                rel = cls(StructTag.unbound_relationship, rel.id, rel.rel_type,
                          rel.properties)
                if rel not in rels:
                    rels.append(rel)
            ids = [e.id for e in jolt.path]
            return cls(StructTag.path, nodes, rels, ids)
        raise TypeError("Unsupported jolt type: {}".format(type(jolt)))

    def to_jolt_type(self):
        if not self._verified:
            raise ValueError("Can only convert verified struct to jolt type")
        if self.tag == StructTag.date:
            return jolt_types.JoltDate.new(*self.fields)
        if self.tag == StructTag.time:
            return jolt_types.JoltTime.new(*self.fields)
        if self.tag == StructTag.local_time:
            return jolt_types.JoltLocalTime.new(*self.fields)
        if self.tag == StructTag.date_time:
            return jolt_types.JoltDateTime.new(*self.fields)
        if self.tag == StructTag.local_date_time:
            return jolt_types.JoltLocalDateTime.new(*self.fields)
        if self.tag == StructTag.duration:
            return jolt_types.JoltDuration.new(*self.fields)
        if self.tag == StructTag.point_2d or self.tag == StructTag.point_3d:
            return jolt_types.JoltPoint.new(*self.fields[1:],
                                            srid=self.fields[0])
        if self.tag == StructTag.node:
            return jolt_types.JoltNode(*self.fields)
        if self.tag == StructTag.relationship:
            return jolt_types.JoltRelationship(*(self.fields[i]
                                                 for i in (0, 1, 3, 2, 4)))
        if self.tag == StructTag.path:
            nodes = {node.fields[0]: node
                     for node in self.fields[0]}
            rels = {rel.fields[0]: rel
                    for rel in self.fields[1]}
            ids = self.fields[2]
            path = []
            for idx, id_ in enumerate(ids):
                if idx % 2 == 0:
                    path.append(nodes[id_].to_jolt_type())
                else:
                    path.append(jolt_types.JoltRelationship(
                        id_, ids[idx - 1], rels[id_].fields[1],
                        ids[idx + 1], rels[id_].fields[2])
                    )
            return jolt_types.JoltPath(*path)
        raise TypeError("Unsupported struct type: {}".format(self.tag))

    def fields_to_jolt_types(self):
        def transform_field(field):
            if isinstance(field, dict):
                return {k: transform_field(v) for k, v in field.items()}
            if isinstance(field, list):
                return list(map(transform_field, field))
            if isinstance(field, Structure):
                return field.to_jolt_type()
            return field

        return transform_field(self.fields)


class Packer:

    def __init__(self, stream):
        self.stream = stream
        self._write = self.stream.write

    def pack_raw(self, data):
        self._write(data)

    def pack(self, value):
        return self._pack(value)

    def _pack(self, value):
        write = self._write

        # None
        if value is None:
            write(b"\xC0")  # NULL

        # Boolean
        elif value is True:
            write(b"\xC3")
        elif value is False:
            write(b"\xC2")

        # Float (only double precision is supported)
        elif isinstance(value, float):
            write(b"\xC1")
            write(struct_pack(">d", value))

        # Integer
        elif isinstance(value, int):
            if -0x10 <= value < 0x80:
                write(PACKED_UINT_8[value % 0x100])
            elif -0x80 <= value < -0x10:
                write(b"\xC8")
                write(PACKED_UINT_8[value % 0x100])
            elif -0x8000 <= value < 0x8000:
                write(b"\xC9")
                write(PACKED_UINT_16[value % 0x10000])
            elif -0x80000000 <= value < 0x80000000:
                write(b"\xCA")
                write(struct_pack(">i", value))
            elif INT64_MIN <= value < INT64_MAX:
                write(b"\xCB")
                write(struct_pack(">q", value))
            else:
                raise OverflowError("Integer %s out of range" % value)

        # String
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            self.pack_string_header(len(encoded))
            self.pack_raw(encoded)

        # Bytes
        elif isinstance(value, bytes):
            self.pack_bytes_header(len(value))
            self.pack_raw(value)
        elif isinstance(value, bytearray):
            self.pack_bytes_header(len(value))
            self.pack_raw(bytes(value))

        # List
        elif isinstance(value, list):
            self.pack_list_header(len(value))
            for item in value:
                self._pack(item)

        # Map
        elif isinstance(value, dict):
            self.pack_map_header(len(value))
            for key, item in value.items():
                self._pack(key)
                self._pack(item)

        # Structure
        elif isinstance(value, Structure):
            self.pack_struct(value.tag, value.fields)

        # Other
        else:
            raise ValueError("Values of type %s are not supported"
                             % type(value))

    def pack_bytes_header(self, size):
        write = self._write
        if size < 0x100:
            write(b"\xCC")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xCD")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xCE")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("Bytes header size out of range")

    def pack_string_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\x80")
        elif size == 0x01:
            write(b"\x81")
        elif size == 0x02:
            write(b"\x82")
        elif size == 0x03:
            write(b"\x83")
        elif size == 0x04:
            write(b"\x84")
        elif size == 0x05:
            write(b"\x85")
        elif size == 0x06:
            write(b"\x86")
        elif size == 0x07:
            write(b"\x87")
        elif size == 0x08:
            write(b"\x88")
        elif size == 0x09:
            write(b"\x89")
        elif size == 0x0A:
            write(b"\x8A")
        elif size == 0x0B:
            write(b"\x8B")
        elif size == 0x0C:
            write(b"\x8C")
        elif size == 0x0D:
            write(b"\x8D")
        elif size == 0x0E:
            write(b"\x8E")
        elif size == 0x0F:
            write(b"\x8F")
        elif size < 0x100:
            write(b"\xD0")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD1")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xD2")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("String header size out of range")

    def pack_list_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\x90")
        elif size == 0x01:
            write(b"\x91")
        elif size == 0x02:
            write(b"\x92")
        elif size == 0x03:
            write(b"\x93")
        elif size == 0x04:
            write(b"\x94")
        elif size == 0x05:
            write(b"\x95")
        elif size == 0x06:
            write(b"\x96")
        elif size == 0x07:
            write(b"\x97")
        elif size == 0x08:
            write(b"\x98")
        elif size == 0x09:
            write(b"\x99")
        elif size == 0x0A:
            write(b"\x9A")
        elif size == 0x0B:
            write(b"\x9B")
        elif size == 0x0C:
            write(b"\x9C")
        elif size == 0x0D:
            write(b"\x9D")
        elif size == 0x0E:
            write(b"\x9E")
        elif size == 0x0F:
            write(b"\x9F")
        elif size < 0x100:
            write(b"\xD4")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD5")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xD6")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("List header size out of range")

    def pack_list_stream_header(self):
        self._write(b"\xD7")

    def pack_map_header(self, size):
        write = self._write
        if size == 0x00:
            write(b"\xA0")
        elif size == 0x01:
            write(b"\xA1")
        elif size == 0x02:
            write(b"\xA2")
        elif size == 0x03:
            write(b"\xA3")
        elif size == 0x04:
            write(b"\xA4")
        elif size == 0x05:
            write(b"\xA5")
        elif size == 0x06:
            write(b"\xA6")
        elif size == 0x07:
            write(b"\xA7")
        elif size == 0x08:
            write(b"\xA8")
        elif size == 0x09:
            write(b"\xA9")
        elif size == 0x0A:
            write(b"\xAA")
        elif size == 0x0B:
            write(b"\xAB")
        elif size == 0x0C:
            write(b"\xAC")
        elif size == 0x0D:
            write(b"\xAD")
        elif size == 0x0E:
            write(b"\xAE")
        elif size == 0x0F:
            write(b"\xAF")
        elif size < 0x100:
            write(b"\xD8")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xD9")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xDA")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("Map header size out of range")

    def pack_map_stream_header(self):
        self._write(b"\xDB")

    def pack_struct(self, signature, fields):
        if len(signature) != 1 or not isinstance(signature, bytes):
            raise ValueError("Structure signature must be a single byte value")
        write = self._write
        size = len(fields)
        if size == 0x00:
            write(b"\xB0")
        elif size == 0x01:
            write(b"\xB1")
        elif size == 0x02:
            write(b"\xB2")
        elif size == 0x03:
            write(b"\xB3")
        elif size == 0x04:
            write(b"\xB4")
        elif size == 0x05:
            write(b"\xB5")
        elif size == 0x06:
            write(b"\xB6")
        elif size == 0x07:
            write(b"\xB7")
        elif size == 0x08:
            write(b"\xB8")
        elif size == 0x09:
            write(b"\xB9")
        elif size == 0x0A:
            write(b"\xBA")
        elif size == 0x0B:
            write(b"\xBB")
        elif size == 0x0C:
            write(b"\xBC")
        elif size == 0x0D:
            write(b"\xBD")
        elif size == 0x0E:
            write(b"\xBE")
        elif size == 0x0F:
            write(b"\xBF")
        else:
            raise OverflowError("Structure size out of range")
        write(signature)
        for field in fields:
            self._pack(field)

    def pack_end_of_stream(self):
        self._write(b"\xDF")


class Unpacker:

    def __init__(self, unpackable, bolt_version):
        self.unpackable = unpackable
        self.bolt_version = bolt_version

    def reset(self):
        self.unpackable.reset()

    def read(self, n=1):
        return self.unpackable.read(n)

    def read_u8(self):
        return self.unpackable.read_u8()

    def unpack_message(self):
        res = self._unpack(verify_struct=False)
        if not isinstance(res, Structure):
            raise ValueError("Expected a message struct")
        return res

    def unpack(self):
        return self._unpack()

    def _unpack(self, verify_struct=True):
        marker = self.read_u8()

        if marker == -1:
            raise ValueError("Nothing to unpack")

        # Tiny Integer
        if 0x00 <= marker <= 0x7F:
            return marker
        elif 0xF0 <= marker <= 0xFF:
            return marker - 0x100

        # Null
        elif marker == 0xC0:
            return None

        # Float
        elif marker == 0xC1:
            value, = struct_unpack(">d", self.read(8))
            return value

        # Boolean
        elif marker == 0xC2:
            return False
        elif marker == 0xC3:
            return True

        # Integer
        elif marker == 0xC8:
            return struct_unpack(">b", self.read(1))[0]
        elif marker == 0xC9:
            return struct_unpack(">h", self.read(2))[0]
        elif marker == 0xCA:
            return struct_unpack(">i", self.read(4))[0]
        elif marker == 0xCB:
            return struct_unpack(">q", self.read(8))[0]

        # Bytes
        elif marker == 0xCC:
            size, = struct_unpack(">B", self.read(1))
            return self.read(size).tobytes()
        elif marker == 0xCD:
            size, = struct_unpack(">H", self.read(2))
            return self.read(size).tobytes()
        elif marker == 0xCE:
            size, = struct_unpack(">I", self.read(4))
            return self.read(size).tobytes()

        else:
            marker_high = marker & 0xF0
            # String
            if marker_high == 0x80:  # TINY_STRING
                return decode(self.read(marker & 0x0F), "utf-8")
            elif marker == 0xD0:  # STRING_8:
                size, = struct_unpack(">B", self.read(1))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD1:  # STRING_16:
                size, = struct_unpack(">H", self.read(2))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD2:  # STRING_32:
                size, = struct_unpack(">I", self.read(4))
                return decode(self.read(size), "utf-8")

            # List
            elif 0x90 <= marker <= 0x9F or 0xD4 <= marker <= 0xD7:
                return list(self._unpack_list_items(marker))

            # Map
            elif 0xA0 <= marker <= 0xAF or 0xD8 <= marker <= 0xDB:
                return self._unpack_map(marker)

            # Structure
            elif 0xB0 <= marker <= 0xBF:
                size, tag = self._unpack_structure_header(marker)
                fields = [None] * size
                for i in range(len(fields)):
                    fields[i] = self._unpack(verify_struct=True)
                return Structure(tag, *fields, verified=verify_struct,
                                 bolt_version=self.bolt_version)

            elif marker == 0xDF:  # END_OF_STREAM:
                return EndOfStream

            else:
                raise ValueError("Unknown PackStream marker %02X" % marker)

    def _unpack_list_items(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0x90:
            size = marker & 0x0F
            if size == 0:
                return
            elif size == 1:
                yield self._unpack()
            else:
                for _ in range(size):
                    yield self._unpack()
        elif marker == 0xD4:  # LIST_8:
            size, = struct_unpack(">B", self.read(1))
            for _ in range(size):
                yield self._unpack()
        elif marker == 0xD5:  # LIST_16:
            size, = struct_unpack(">H", self.read(2))
            for _ in range(size):
                yield self._unpack()
        elif marker == 0xD6:  # LIST_32:
            size, = struct_unpack(">I", self.read(4))
            for _ in range(size):
                yield self._unpack()
        elif marker == 0xD7:  # LIST_STREAM:
            item = None
            while item is not EndOfStream:
                item = self._unpack()
                if item is not EndOfStream:
                    yield item
        else:
            return

    def unpack_map(self):
        marker = self.read_u8()
        return self._unpack_map(marker)

    def _unpack_map(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xA0:
            size = marker & 0x0F
            value = {}
            for _ in range(size):
                key = self._unpack()
                value[key] = self._unpack()
            return value
        elif marker == 0xD8:  # MAP_8:
            size, = struct_unpack(">B", self.read(1))
            value = {}
            for _ in range(size):
                key = self._unpack()
                value[key] = self._unpack()
            return value
        elif marker == 0xD9:  # MAP_16:
            size, = struct_unpack(">H", self.read(2))
            value = {}
            for _ in range(size):
                key = self._unpack()
                value[key] = self._unpack()
            return value
        elif marker == 0xDA:  # MAP_32:
            size, = struct_unpack(">I", self.read(4))
            value = {}
            for _ in range(size):
                key = self._unpack()
                value[key] = self._unpack()
            return value
        elif marker == 0xDB:  # MAP_STREAM:
            value = {}
            key = None
            while key is not EndOfStream:
                key = self._unpack()
                if key is not EndOfStream:
                    value[key] = self._unpack()
            return value
        else:
            return None

    def unpack_structure_header(self):
        marker = self.read_u8()
        if marker == -1:
            return None, None
        else:
            return self._unpack_structure_header(marker)

    def _unpack_structure_header(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xB0:  # TINY_STRUCT
            signature = self.read(1).tobytes()
            return marker & 0x0F, signature
        else:
            raise ValueError("Expected structure, found marker %02X" % marker)


class UnpackableBuffer:

    initial_capacity = 8192

    def __init__(self, data=None):
        if data is None:
            self.data = bytearray(self.initial_capacity)
            self.used = 0
        else:
            self.data = bytearray(data)
            self.used = len(self.data)
        self.p = 0

    def reset(self):
        self.used = 0
        self.p = 0

    def read(self, n=1):
        view = memoryview(self.data)
        q = self.p + n
        subview = view[self.p:q]
        self.p = q
        return subview

    def read_u8(self):
        if self.used - self.p >= 1:
            value = self.data[self.p]
            self.p += 1
            return value
        else:
            return -1

    def pop_u16(self):
        """Remove and return last 2 bytes as big-endian 16 bit unsigned int."""
        if self.used >= 2:
            value = 0x100 * self.data[self.used - 2] + self.data[self.used - 1]
            self.used -= 2
            return value
        else:
            return -1

    def receive(self, sock, n_bytes):
        end = self.used + n_bytes
        if end > len(self.data):
            self.data += bytearray(end - len(self.data))
        view = memoryview(self.data)
        while self.used < end:
            n = sock.recv_into(view[self.used:end], end - self.used)
            if n == 0:
                raise OSError("No data")
            self.used += n


class PackStream:
    """Chunked message reader/writer for PackStream messaging."""

    def __init__(self, wire, bolt_version):
        self.wire = wire
        self.bolt_version = bolt_version
        self.data_buffer = []
        self.next_chunk_size = None

    def read_message(self):
        """Read a chunked message.

        :return:
        """
        while True:
            if self.next_chunk_size is None:
                chunk_size = self.wire.read(2)
                self.next_chunk_size, = struct_unpack(">H", chunk_size)
            if self.next_chunk_size:
                chunk_data = self.wire.read(self.next_chunk_size)
                self.next_chunk_size = None
                self.data_buffer.append(chunk_data)
            else:
                self.next_chunk_size = None
                break
        buffer = UnpackableBuffer(b"".join(self.data_buffer))
        self.data_buffer = []
        unpacker = Unpacker(buffer, self.bolt_version)
        return unpacker.unpack_message()

    def write_message(self, message):
        """Write a chunked message.

        :param message:
        :return:
        """
        if not isinstance(message, Structure):
            raise TypeError("Message must be a Structure instance")
        b = BytesIO()
        packer = Packer(b)
        packer.pack(message)
        data = b.getvalue()
        while len(data) > 65535:
            chunk = data[:65535]
            header = bytearray(divmod(len(chunk), 0x100))
            self.wire.write(header + chunk)
            data = data[65535:]
        header = bytearray(divmod(len(data), 0x100))
        self.wire.write(header + data + b"\x00\x00")

    def drain(self):
        """Flush the writer.

        :return:
        """
        self.wire.send()

    def close(self):
        """Close.

        :return:
        """
        self.wire.close()
