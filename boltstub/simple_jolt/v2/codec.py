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


import inspect

from ..common.errors import (
    JOLTValueError,
    NoSimpleRepresentation,
)

# unused transformer imports are required for the codec to pick them up
from ..v1.codec import JoltBoolTransformer  # noqa: F401
from ..v1.codec import JoltBytesTransformer  # noqa: F401
from ..v1.codec import JoltDictTransformer  # noqa: F401
from ..v1.codec import JoltFloatTransformer  # noqa: F401
from ..v1.codec import JoltIntTransformer  # noqa: F401
from ..v1.codec import JoltListTransformer  # noqa: F401
from ..v1.codec import JoltNullTransformer  # noqa: F401
from ..v1.codec import JoltStrTransformer  # noqa: F401
from ..v1.codec import (
    Codec,
    JoltDateTimeTransformer,
    JoltNodeTransformer,
    JoltPathTransformer,
    JoltPointTransformer,
    JoltRelationTransformer,
    JoltReverseRelationTransformer,
    JoltTypeTransformer,
)
from .types import (
    JoltDate,
    JoltDateTime,
    JoltDuration,
    JoltLocalDateTime,
    JoltLocalTime,
    JoltNode,
    JoltPath,
    JoltPoint,
    JoltRelationship,
    JoltTime,
)


class JoltDateTimeTransformer(JoltDateTimeTransformer):
    _supported_types = (
        JoltDate, JoltTime, JoltLocalTime,
        JoltDateTime, JoltLocalDateTime, JoltDuration
    )

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expecting str after sigil "T"')
        if value.startswith("P"):
            return JoltDuration(value)
        if ":" in value:
            if "T" in value:
                clss = JoltDateTime, JoltLocalDateTime
            else:
                clss = JoltTime, JoltLocalTime
        else:
            clss = JoltDate,
        for cls in clss:
            try:
                return cls(value)
            except ValueError:
                pass
        raise JOLTValueError("Couldn't parse {} as any of {}"
                             .format(value, clss))


class JoltPointTransformer(JoltPointTransformer):
    _supported_types = JoltPoint,

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expecting str sigil "@"')
        return JoltPoint(value)


class JoltNodeTransformer(JoltNodeTransformer):
    _supported_types = JoltNode,

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list) or len(value) != 4:
            raise JOLTValueError('Expecting list of length 4 after sigil "()"')
        id_, labels, properties, element_id = value
        if not isinstance(id_, int) and id_ is not None:
            raise JOLTValueError("Node id must be int or none")
        if not isinstance(labels, list):
            raise JOLTValueError("Node labels must be list")
        if not all(map(lambda label: isinstance(label, str), labels)):
            raise JOLTValueError("Node labels must be list of str")
        if not isinstance(properties, dict):
            raise JOLTValueError("Node properties must be dict")
        if not isinstance(element_id, str):
            raise JOLTValueError("Node element_id must be a str")

        properties = {k: decode_cb(v) for k, v in properties.items()}
        assert all(map(lambda e: isinstance(e, str), properties.keys()))
        return JoltNode(id_, labels, properties, element_id)

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: [
            value.id,
            value.labels,
            {k: encode_cb(v) for k, v in value.properties.items()},
            value.element_id
        ]}


class JoltRelationTransformer(JoltRelationTransformer):
    _supported_types = JoltRelationship,

    @classmethod
    def _decode_full(cls, value, decode_cb):
        if not isinstance(value, list) or len(value) != 8:
            raise JOLTValueError('Expecting list of length 8 after sigil "%s"'
                                 % cls.sigil)
        id_, start_node_id, rel_type, end_node_id, properties, element_id, \
            start_node_element_id, end_node_element_id = value

        if not isinstance(id_, int) and id_ is not None:
            raise JOLTValueError("Relationship id must be int or None")
        if not isinstance(start_node_id, int) and start_node_id is not None:
            raise JOLTValueError("Relationship's start id must be int or None")
        if not isinstance(rel_type, str):
            raise JOLTValueError("Relationship's type id must be str")
        if not isinstance(end_node_id, int) and end_node_id is not None:
            raise JOLTValueError("Relationship's end id must be int or None")
        if not isinstance(properties, dict):
            raise JOLTValueError("Relationship's properties  must be dict")
        properties = {k: decode_cb(v) for k, v in properties.items()}
        if not all(map(lambda e: isinstance(e, str), properties.keys())):
            raise JOLTValueError("Relationship's properties keys must be str")
        if not isinstance(element_id, str):
            raise JOLTValueError("Relationship's element_id must be str")
        if not isinstance(start_node_element_id, str):
            raise JOLTValueError("Relationship's start_node_element_id must "
                                 "be str")
        if not isinstance(end_node_element_id, str):
            raise JOLTValueError("Relationship's end_node_element_id must "
                                 "be str")

        return JoltRelationship(id_, start_node_id, rel_type, end_node_id,
                                properties, element_id, start_node_element_id,
                                end_node_element_id)

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: [
            value.id, value.start_node_id, value.rel_type, value.end_node_id,
            {k: encode_cb(v) for k, v in value.properties.items()},
            value.element_id, value.start_node_element_id,
            value.end_node_element_id
        ]}


class JoltReverseRelationTransformer(JoltReverseRelationTransformer):
    sigil = "<-"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @classmethod
    def _decode_full(cls, value, decode_cb):
        if not isinstance(value, list) or len(value) != 8:
            raise JOLTValueError('Expecting list of length 8 after sigil "%s"'
                                 % cls.sigil)
        value = [value[idx] for idx in (0, 3, 2, 1, 4, 5, 7, 6)]
        return JoltRelationTransformer.decode_full(value, decode_cb)


class JoltPathTransformer(JoltPathTransformer):
    _supported_types = JoltPath,

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list):
            raise JOLTValueError('Expecting list after sigil ".."')
        path = list(map(decode_cb, value))
        if path and len(path) % 2 != 1:
            raise JOLTValueError("Path doesn't have odd number of elements")
        for i in range(0, len(path), 2):
            node = path[i]
            if not isinstance(node, JoltNode):
                raise JOLTValueError("Element %i in path was expected to be "
                                     "a Node" % i)
            if i != 0:
                prev_rel = path[i - 1]
                if not isinstance(prev_rel, JoltRelationship):
                    raise JOLTValueError("Element %i in path was expected to "
                                         "be a Relationship" % (i - 1))
                if prev_rel.end_node_element_id != node.element_id:
                    raise JOLTValueError(
                        "Relationship %i did not point to the following Node "
                        "in the path" % (i - 1)
                    )

            if i < len(path) - 1:
                next_rel = path[i + 1]
                if not isinstance(next_rel, JoltRelationship):
                    raise JOLTValueError(
                        "Element %i in path was expected to be a Relationship"
                        % (i + 1)
                    )
                if next_rel.start_node_element_id != node.element_id:
                    raise JOLTValueError(
                        "Relationship %i did not point to the previous Node "
                        "in the path" % (i + 1)
                    )
        return JoltPath(*path)


class Codec(Codec):
    sigil_to_type = {
        cls.sigil: cls
        for cls in globals().values()
        if (inspect.isclass(cls) and issubclass(cls, JoltTypeTransformer)
            and cls.sigil is not None)
    }
    native_to_type = {
        type_: cls
        for cls in globals().values()
        if (inspect.isclass(cls) and issubclass(cls, JoltTypeTransformer)
            and cls._supported_types)
        for type_ in cls._supported_types
    }


decode = Codec.decode
encode_simple = Codec.encode_simple
encode_full = Codec.encode_full


__all__ = [
    Codec,
    decode,
    encode_simple,
    encode_full,
]
