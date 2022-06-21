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
import inspect
from io import BytesIO
import re
from struct import pack as struct_pack
from struct import unpack as struct_unpack

from .simple_jolt.common import types as jolt_common_types
from .simple_jolt.v1 import types as jolt_v1_types
from .simple_jolt.v2 import types as jolt_v2_types

_jolt_types = {
    1: jolt_v1_types,
    2: jolt_v2_types,
}


def jolt_types(packstream_version):
    if not packstream_version:
        raise ValueError(
            "JOLT conversion requires packstream_version to be specified"
        )
    return _jolt_types[packstream_version]


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


class StructTagV1:
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


class StructTagV2(StructTagV1):
    date_time = b"\x49"
    date_time_zone_id = b"\x69"


class Structure:

    def __init__(self, tag, *fields, packstream_version=None, verified=True):
        self.tag = tag
        self.fields = list(fields)
        self._packstream_version = packstream_version
        self._verified = verified
        if packstream_version not in (None, 1, 2):
            raise ValueError("Unknown packstream version: %s"
                             % packstream_version)

        if verified:
            if not packstream_version:
                raise ValueError(
                    "packstream_version is required to verify the Structure"
                )
            self._verify()

    def _verify(self):
        if self._packstream_version == 1:
            PackstreamV1StructureValidator.verify_fields(self)
        elif self._packstream_version == 2:
            PackstreamV2StructureValidator.verify_fields(self)

    @property
    def verified(self):
        return self._verified

    @property
    def packstream_version(self):
        return self._packstream_version

    def __repr__(self):
        if self.packstream_version:
            return "StructureV%i[0x%02X](%s)" % (
                self.packstream_version,
                ord(self.tag), ", ".join(map(repr, self.fields))
            )
        return "Structure[0x%02X](%s)" % (
            ord(self.tag), ", ".join(map(repr, self.fields))
        )

    def __eq__(self, other):
        try:
            assert all(
                StructTagV1.path == value.path
                for key, value in locals().items()
                if re.match(r"^StructTagV[1-9]\d*$", key)
            )
            if self.tag == StructTagV1.path:
                # path struct => order of nodes and rels is irrelevant
                return (other.tag == self.tag
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
            self._verify()

    def match_jolt_wildcard(self, wildcard: jolt_common_types.JoltWildcard):
        jolt_types_ = jolt_types(self._packstream_version)
        struct_tags = (StructTagV1 if self._packstream_version == 1
                       else StructTagV2)
        for t in wildcard.types:
            if issubclass(t, jolt_types_.JoltDate):
                if self.tag == struct_tags.date:
                    return True
            elif issubclass(t, jolt_types_.JoltTime):
                if self.tag == struct_tags.time:
                    return True
            elif issubclass(t, jolt_types_.JoltLocalTime):
                if self.tag == struct_tags.local_time:
                    return True
            elif issubclass(t, jolt_types_.JoltDateTime):
                if self.tag in (struct_tags.date_time,
                                struct_tags.date_time_zone_id):
                    return True
            elif issubclass(t, jolt_types_.JoltLocalDateTime):
                if self.tag == struct_tags.local_date_time:
                    return True
            elif issubclass(t, jolt_types_.JoltDuration):
                if self.tag == struct_tags.duration:
                    return True
            elif issubclass(t, jolt_types_.JoltPoint):
                if self.tag in (struct_tags.point_2d, struct_tags.point_3d):
                    return True
            elif issubclass(t, jolt_types_.JoltNode):
                if self.tag == struct_tags.node:
                    return True
            elif issubclass(t, jolt_types_.JoltRelationship):
                if self.tag == struct_tags.relationship:
                    return True
            elif issubclass(t, jolt_types_.JoltPath):
                if self.tag == struct_tags.path:
                    return True

    @classmethod
    def _from_jolt_v1_type(cls, jolt: jolt_v1_types.JoltType):
        if isinstance(jolt, jolt_v1_types.JoltDate):
            return cls(StructTagV1.date, jolt.days, packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltTime):
            return cls(StructTagV1.time, jolt.nanoseconds, jolt.utc_offset,
                       packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltLocalTime):
            return cls(StructTagV1.local_time, jolt.nanoseconds,
                       packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltDateTime):
            if jolt.time.zone_id:
                return cls(StructTagV1.date_time_zone_id,
                           *jolt.seconds_nanoseconds, jolt.time.zone_id,
                           packstream_version=1)
            else:
                return cls(StructTagV1.date_time, *jolt.seconds_nanoseconds,
                           jolt.time.utc_offset, packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltLocalDateTime):
            return cls(StructTagV1.local_date_time, *jolt.seconds_nanoseconds,
                       packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltDuration):
            return cls(StructTagV1.duration, jolt.months, jolt.days,
                       jolt.seconds, jolt.nanoseconds, packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltPoint):
            if jolt.z is None:  # 2D
                return cls(StructTagV1.point_2d, jolt.srid, jolt.x, jolt.y,
                           packstream_version=1)
            else:
                return cls(StructTagV1.point_3d, jolt.srid, jolt.x, jolt.y,
                           jolt.z, packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltNode):
            return cls(StructTagV1.node, jolt.id, jolt.labels, jolt.properties,
                       packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltRelationship):
            return cls(StructTagV1.relationship, jolt.id, jolt.start_node_id,
                       jolt.end_node_id, jolt.rel_type, jolt.properties,
                       packstream_version=1)
        if isinstance(jolt, jolt_v1_types.JoltPath):
            uniq_nodes = []
            uniq_rels = []
            ids = []
            nodes = []
            rels = []
            # Node structs
            node_idxs = {}
            node_idx = 0
            for node in jolt.path[::2]:
                nodes.append(node)
                map_node = Structure._from_jolt_v1_type(node)
                if map_node not in uniq_nodes:
                    node_idxs[node.id] = node_idx
                    node_idx = node_idx + 1
                    uniq_nodes.append(map_node)

            # UnboundRelationship structs

            rel_idxs = {}
            rel_idx = 1
            for rel in jolt.path[1::2]:
                rels.append(rel)

                ub_rel = cls(StructTagV1.unbound_relationship, rel.id,
                             rel.rel_type, rel.properties,
                             packstream_version=1)
                if ub_rel not in uniq_rels:
                    rel_idxs[rel.id] = rel_idx
                    rel_idx = rel_idx + 1
                    uniq_rels.append(ub_rel)

            last_node = nodes[0]
            for i in range(1, (len(rels) * 2) + 1):
                if i % 2 == 0:
                    last_node = nodes[i // 2]
                    index = node_idxs[last_node.id]
                    ids.append(index)
                else:
                    rel = rels[i // 2]
                    index = rel_idxs[rel.id]
                    if last_node.id == rel.start_node_id:
                        ids.append(index)
                    else:
                        ids.append(-index)

            return cls(StructTagV1.path, uniq_nodes, uniq_rels, ids,
                       packstream_version=1)
        raise TypeError("Unsupported jolt type: {}".format(type(jolt)))

    @classmethod
    def _from_jolt_v2_type(cls, jolt: jolt_v1_types.JoltType):
        if isinstance(jolt, jolt_v2_types.JoltDate):
            return cls(StructTagV2.date, jolt.days, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltTime):
            return cls(StructTagV2.time, jolt.nanoseconds, jolt.utc_offset,
                       packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltLocalTime):
            return cls(StructTagV2.local_time, jolt.nanoseconds,
                       packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltDateTime):
            if jolt.time.zone_id:
                return cls(StructTagV2.date_time_zone_id,
                           *jolt.seconds_nanoseconds, jolt.time.zone_id,
                           packstream_version=2)
            else:
                return cls(StructTagV2.date_time, *jolt.seconds_nanoseconds,
                           jolt.time.utc_offset, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltLocalDateTime):
            return cls(StructTagV2.local_date_time, *jolt.seconds_nanoseconds,
                       packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltDuration):
            return cls(StructTagV2.duration, jolt.months, jolt.days,
                       jolt.seconds, jolt.nanoseconds, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltPoint):
            if jolt.z is None:  # 2D
                return cls(StructTagV2.point_2d, jolt.srid, jolt.x, jolt.y,
                           packstream_version=2)
            else:
                return cls(StructTagV2.point_3d, jolt.srid, jolt.x, jolt.y,
                           jolt.z, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltNode):
            return cls(StructTagV2.node, jolt.id, jolt.labels,
                       jolt.properties, jolt.element_id, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltRelationship):
            return cls(StructTagV2.relationship, jolt.id, jolt.start_node_id,
                       jolt.end_node_id, jolt.rel_type, jolt.properties,
                       jolt.element_id, jolt.start_node_element_id,
                       jolt.end_node_element_id, packstream_version=2)
        if isinstance(jolt, jolt_v2_types.JoltPath):
            uniq_nodes = []
            uniq_rels = []
            ids = []
            nodes = []
            rels = []
            # Node structs
            node_idxs = {}
            node_idx = 0
            for node in jolt.path[::2]:
                nodes.append(node)
                map_node = Structure._from_jolt_v2_type(node)
                if map_node not in uniq_nodes:
                    node_idxs[node.element_id] = node_idx
                    node_idx = node_idx + 1
                    uniq_nodes.append(map_node)

            # UnboundRelationship structs

            rel_idxs = {}
            rel_idx = 1
            for rel in jolt.path[1::2]:
                rels.append(rel)

                ub_rel = cls(StructTagV2.unbound_relationship, rel.id,
                             rel.rel_type, rel.properties, rel.element_id,
                             packstream_version=2)
                if ub_rel not in uniq_rels:
                    rel_idxs[rel.element_id] = rel_idx
                    rel_idx = rel_idx + 1
                    uniq_rels.append(ub_rel)

            last_node = nodes[0]
            for i in range(1, (len(rels) * 2) + 1):
                if i % 2 == 0:
                    last_node = nodes[i // 2]
                    index = node_idxs[last_node.element_id]
                    ids.append(index)
                else:
                    rel = rels[i // 2]
                    index = rel_idxs[rel.element_id]
                    if last_node.id == rel.start_node_id:
                        ids.append(index)
                    else:
                        ids.append(-index)

            return cls(StructTagV2.path, uniq_nodes, uniq_rels, ids,
                       packstream_version=2)
        raise TypeError("Unsupported jolt type: {}".format(type(jolt)))

    @classmethod
    def from_jolt_type(cls, jolt: jolt_common_types.JoltType):
        if isinstance(jolt, jolt_v1_types.JoltType):
            return cls._from_jolt_v1_type(jolt)
        elif isinstance(jolt, jolt_v2_types.JoltType):
            return cls._from_jolt_v2_type(jolt)
        raise TypeError("Unsupported jolt type: {}".format(type(jolt)))

    def _to_jolt_v1_type(self):
        if self.tag == StructTagV1.date:
            return jolt_v1_types.JoltDate.new(*self.fields)
        if self.tag == StructTagV1.time:
            return jolt_v1_types.JoltTime.new(*self.fields)
        if self.tag == StructTagV1.local_time:
            return jolt_v1_types.JoltLocalTime.new(*self.fields)
        if self.tag in (StructTagV1.date_time, StructTagV1.date_time_zone_id):
            return jolt_v1_types.JoltDateTime.new(*self.fields)
        if self.tag == StructTagV1.local_date_time:
            return jolt_v1_types.JoltLocalDateTime.new(*self.fields)
        if self.tag == StructTagV1.duration:
            return jolt_v1_types.JoltDuration.new(*self.fields)
        if self.tag in (StructTagV1.point_2d, StructTagV1.point_3d):
            return jolt_v1_types.JoltPoint.new(*self.fields[1:],
                                               srid=self.fields[0])
        if self.tag == StructTagV1.node:
            return jolt_v1_types.JoltNode(*self.fields)
        if self.tag == StructTagV1.relationship:
            return jolt_v1_types.JoltRelationship(
                *(self.fields[i] for i in (0, 1, 3, 2, 4))
            )
        if self.tag == StructTagV1.path:
            nodes = self.fields[0]
            rels = self.fields[1]
            idxs = self.fields[2]
            last_node = nodes[0]
            path = [nodes[0].to_jolt_type()]
            for i, idx in enumerate(idxs):
                if i % 2 == 0:
                    assert len(idxs) > i + 1
                    rel = rels[abs(idx) - 1]
                    next_node = nodes[abs(idxs[i + 1])]
                    if idx > 0:
                        start_node, end_node = last_node, next_node
                    else:
                        start_node, end_node = next_node, last_node
                    path.append(
                        jolt_v1_types.JoltRelationship(
                            rel.fields[0], start_node.fields[0],
                            rel.fields[1], end_node.fields[0], rel.fields[2],
                        )
                    )
                else:
                    last_node = nodes[idx]
                    path.append(nodes[idx].to_jolt_type())
            return jolt_v1_types.JoltPath(*path)
        raise TypeError("Unsupported struct type: {}".format(self.tag))

    def _to_jolt_v2_type(self):
        if self.tag == StructTagV2.date:
            return jolt_v2_types.JoltDate.new(*self.fields)
        if self.tag == StructTagV2.time:
            return jolt_v2_types.JoltTime.new(*self.fields)
        if self.tag == StructTagV2.local_time:
            return jolt_v2_types.JoltLocalTime.new(*self.fields)
        if self.tag in (StructTagV2.date_time, StructTagV2.date_time_zone_id):
            return jolt_v2_types.JoltDateTime.new(*self.fields)
        if self.tag == StructTagV2.local_date_time:
            return jolt_v2_types.JoltLocalDateTime.new(*self.fields)
        if self.tag == StructTagV2.duration:
            return jolt_v2_types.JoltDuration.new(*self.fields)
        if self.tag in (StructTagV2.point_2d, StructTagV2.point_3d):
            return jolt_v2_types.JoltPoint.new(*self.fields[1:],
                                               srid=self.fields[0])
        if self.tag == StructTagV2.node:
            return jolt_v2_types.JoltNode(*self.fields)
        if self.tag == StructTagV2.relationship:
            return jolt_v2_types.JoltRelationship(
                *(self.fields[i] for i in (0, 1, 3, 2, 4, 5, 6, 7))
            )
        if self.tag == StructTagV2.path:
            nodes = self.fields[0]
            rels = self.fields[1]
            idxs = self.fields[2]
            last_node = nodes[0]
            path = [nodes[0].to_jolt_type()]
            for i, idx in enumerate(idxs):
                if i % 2 == 0:
                    assert len(idxs) > i + 1
                    rel = rels[abs(idx) - 1]
                    next_node = nodes[abs(idxs[i + 1])]
                    if idx > 0:
                        start_node, end_node = last_node, next_node
                    else:
                        start_node, end_node = next_node, last_node
                    path.append(
                        jolt_v2_types.JoltRelationship(
                            rel.fields[0], start_node.fields[0],
                            rel.fields[1], end_node.fields[0], rel.fields[2],
                            rel.fields[3], start_node.fields[3],
                            end_node.fields[3]
                        )
                    )
                else:
                    last_node = nodes[idx]
                    path.append(nodes[idx].to_jolt_type())
            return jolt_v2_types.JoltPath(*path)
        raise TypeError("Unsupported struct type: {}".format(self.tag))

    def to_jolt_type(self):
        if not self._verified:
            raise ValueError("Can only convert verified struct to jolt type")
        if self._packstream_version == 1:
            return self._to_jolt_v1_type()
        elif self._packstream_version == 2:
            return self._to_jolt_v2_type()
        raise ValueError(
            "JOLT encoding is only defined for packstream_version 1 and 2, "
            "not {}".format(self._packstream_version)
        )

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


class PackstreamV1StructureValidator:

    packstream_version = 1

    @classmethod
    def _validate_validations(cls, validations, type_name, structure, fields):
        type_name += f"_v{cls.packstream_version}"
        for validation in validations:
            if not validation(fields):
                raise ValueError(
                    f"Invalid {type_name} struct received.\n"
                    f"validation failed: {inspect.getsource(validation)}"
                    f" {structure}")

    @classmethod
    def _verify_node(cls, structure, fields):
        validations = [
            lambda f: len(f) == 3,
            lambda f: isinstance(f[0], int),
            lambda f: isinstance(f[1], list),
            lambda f: all(isinstance(label, str) for label in f[1]),
            lambda f: isinstance(f[2], dict),
            lambda f: all(isinstance(k, str) for k in f[2].keys())
        ]
        cls._validate_validations(validations, "Node", structure, fields)

    @classmethod
    def _verify_unbound_relationship(cls, structure, fields):
        validations = [
            lambda f: len(f) == 3,
            lambda f: isinstance(f[0], int),
            lambda f: isinstance(f[1], str),
            lambda f: isinstance(f[2], dict),
            lambda f: all(isinstance(k, str) for k in f[2].keys())
        ]
        cls._validate_validations(validations, "UnboundRelationship",
                                  structure, fields)

    @classmethod
    def _verify_relationship(cls, structure, fields):
        validations = [
            lambda f: len(f) == 5,
            lambda f: isinstance(f[0], int),
            lambda f: isinstance(f[1], int),
            lambda f: isinstance(f[2], int),
            lambda f: isinstance(f[3], str),
            lambda f: isinstance(f[4], dict),
            lambda f: all(isinstance(k, str) for k in f[4].keys())
        ]
        cls._validate_validations(validations, "Relationship",
                                  structure, fields)

    @classmethod
    def _verify_path(cls, structure, fields):
        validations = [
            lambda f: len(f) == 3,
            lambda f: all(isinstance(n, Structure)
                          and n.tag == StructTagV1.node
                          and cls.verify_fields(n)
                          for n in f[0]),
            lambda f: isinstance(f[1], list),
            lambda f: all(isinstance(rel, Structure)
                          and rel.tag == StructTagV1.unbound_relationship
                          and cls.verify_fields(rel)
                          for rel in f[1]),
            lambda f: isinstance(f[2], list),
            # index is less than respective array length
            # rels indexed from 1, nodes 0
            lambda f: all(isinstance(id_, int)
                          and abs(id_) <= len(f[1])
                          if i % 2 == 0 else abs(id_) < len(f[0])
                          for i, id_ in enumerate(f[2]))
        ]
        cls._validate_validations(validations, "Path", structure, fields)

    @classmethod
    def _build_generic_verifier(cls, types, name):
        def verify(structure, fields):
            if (len(fields) != len(types)
                or not all(isinstance(f, t)
                           for f, t in zip(fields, types))):
                raise ValueError(
                    "Invalid %s struct received %r" % (name, structure)
                )

        return verify

    @classmethod
    def verify_fields(cls, structure: Structure):
        tag, fields = structure.tag, structure.fields

        field_validator = {
            StructTagV1.node: cls._verify_node,
            StructTagV1.relationship: cls._verify_relationship,
            StructTagV1.unbound_relationship: cls._verify_unbound_relationship,
            StructTagV1.path: cls._verify_path,
            StructTagV1.date: cls._build_generic_verifier((int,), "Date"),
            StructTagV1.time: cls._build_generic_verifier((int, int), "Time"),
            StructTagV1.local_time: cls._build_generic_verifier(
                (int,), "LocalTime"
            ),
            StructTagV1.date_time: cls._build_generic_verifier(
                (int, int, int,), "DateTime"
            ),
            StructTagV1.date_time_zone_id: cls._build_generic_verifier(
                (int, int, str), "DateTimeZoneId"
            ),
            StructTagV1.local_date_time: cls._build_generic_verifier(
                (int, int), "LocalDateTime"
            ),
            StructTagV1.duration: cls._build_generic_verifier(
                (int, int, int, int), "Duration"
            ),
            StructTagV1.point_2d: cls._build_generic_verifier(
                (int, float, float), "Point2D"
            ),
            StructTagV1.point_3d: cls._build_generic_verifier(
                (int, float, float, float), "Point3D"
            ),
        }

        if tag in field_validator:
            field_validator[tag](structure, fields)
        return True


class PackstreamV2StructureValidator(PackstreamV1StructureValidator):

    @classmethod
    def _verify_node(cls, structure, fields):
        validations = [
            lambda f: len(f) == 4,
            lambda f: isinstance(f[0], int) or f[0] is None,
            lambda f: isinstance(f[1], list),
            lambda f: all(isinstance(label, str) for label in f[1]),
            lambda f: isinstance(f[2], dict),
            lambda f: all(isinstance(k, str) for k in f[2].keys()),
            lambda f: isinstance(f[3], str)
        ]
        cls._validate_validations(validations, "Node", structure, fields)

    @classmethod
    def _verify_unbound_relationship(cls, structure, fields):
        validations = [
            lambda f: len(f) == 4,
            lambda f: isinstance(f[0], int) or f[0] is None,
            lambda f: isinstance(f[1], str),
            lambda f: isinstance(f[2], dict),
            lambda f: all(isinstance(k, str) for k in f[2].keys()),
            lambda f: isinstance(f[3], str)
        ]
        cls._validate_validations(validations, "UnboundRelationship",
                                  structure, fields)

    @classmethod
    def _verify_relationship(cls, structure, fields):
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
        cls._validate_validations(validations, "Relationship",
                                  structure, fields)

    @classmethod
    def verify_fields(cls, structure: Structure):
        assert all(
            hasattr(StructTagV1, tag)
            and getattr(StructTagV1, tag) == getattr(StructTagV2, tag)
            for tag in dir(StructTagV2) if not (
                tag.startswith("_")
                or tag in ("date_time", "date_time_zone_id")
            )
        )

        tag, fields = structure.tag, structure.fields

        field_validator = {
            StructTagV2.date_time: cls._build_generic_verifier(
                (int, int, int,), "DateTime"
            ),
            StructTagV2.date_time_zone_id: cls._build_generic_verifier(
                (int, int, str), "DateTimeZoneId"
            ),
        }

        if tag in field_validator:
            return field_validator[tag](structure, fields)
        return super().verify_fields(structure)


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

    def __init__(self, unpackable, packstream_version):
        self.unpackable = unpackable
        self.packstream_version = packstream_version

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
                return Structure(tag, *fields,
                                 packstream_version=self.packstream_version,
                                 verified=verify_struct)

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

    def __init__(self, wire, packstream_version):
        self.wire = wire
        self.packstream_version = packstream_version
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
        unpacker = Unpacker(buffer, self.packstream_version)
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
