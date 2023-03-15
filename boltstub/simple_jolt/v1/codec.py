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


import abc
import importlib
import inspect
import re
import sys

from ..common.errors import (
    JOLTValueError,
    NoFullRepresentation,
    NoSimpleRepresentation,
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
    JoltWildcard,
)


class JoltTypeTransformer(abc.ABC):
    _supported_types = ()
    sigil = None

    @staticmethod
    @abc.abstractmethod
    def _decode_simple(value, decode_cb):
        pass

    @classmethod
    def decode_simple(cls, value, decode_cb):
        return cls._decode_simple(value, decode_cb)

    @staticmethod
    @abc.abstractmethod
    def _decode_full(value, decode_cb):
        pass

    @classmethod
    def decode_full(cls, value, decode_cb):
        if value == "*":
            return JoltWildcard(cls._supported_types)
        return cls._decode_full(value, decode_cb)

    @staticmethod
    @abc.abstractmethod
    def _encode_simple(value, encode_cb, human_readable):
        pass

    @classmethod
    def encode_simple(cls, value, encode_cb, human_readable=False):
        return cls._encode_simple(value, encode_cb,
                                  human_readable=human_readable)

    @staticmethod
    @abc.abstractmethod
    def _encode_full(value, encode_cb, human_readable):
        pass

    @classmethod
    def encode_full(cls, value, encode_cb, human_readable=False):
        return cls._encode_full(value, encode_cb,
                                human_readable=human_readable)


class JoltNullTransformer(JoltTypeTransformer):
    _supported_types = type(None),

    @staticmethod
    @abc.abstractmethod
    def _decode_simple(value, decode_cb):
        assert value is None
        return None

    @staticmethod
    @abc.abstractmethod
    def _decode_full(value, decode_cb):
        raise NoFullRepresentation()

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        assert value is None
        return None

    @staticmethod
    def _encode_full(value, encode_cb, human_readable):
        raise NoFullRepresentation()


class JoltBoolTransformer(JoltTypeTransformer):
    _supported_types = bool,
    sigil = "?"

    @staticmethod
    def _decode_simple(value, decode_cb):
        assert isinstance(value, bool)
        return value

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, bool):
            raise JOLTValueError('Expected bool type after sigil "?"')
        return value

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        assert isinstance(value, bool)
        return value

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, bool)
        return {cls.sigil: value}


class JoltIntTransformer(JoltTypeTransformer):
    _supported_types = int,
    sigil = "Z"

    @staticmethod
    def _decode_simple(value, decode_cb):
        assert isinstance(value, int)
        if -0x80000000 <= value < 0x80000000:
            return value
        else:
            return float(value)

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expected str type after sigil "Z"')
        value = re.sub(r"\.\d*$", "", value)
        try:
            return int(value)
        except ValueError as e:
            raise JOLTValueError("Invalid int representation") from e

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        assert isinstance(value, int)
        if -0x80000000 <= value < 0x80000000:
            return value
        else:
            raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, int)
        return {cls.sigil: str(value)}


class JoltFloatTransformer(JoltTypeTransformer):
    _supported_types = float,
    sigil = "R"

    _constant_translation = {
        "inf": "+Infinity",
        "-inf": "-Infinity",
        "nan": "NaN",
    }

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expected str type after sigil "R"')
        try:
            return float(value)
        except ValueError as e:
            raise JOLTValueError("Invalid float representation") from e

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, float)
        str_repr = str(float(value))
        return {"R": cls._constant_translation.get(str_repr, str_repr)}


class JoltStrTransformer(JoltTypeTransformer):
    _supported_types = str,
    sigil = "U"

    @staticmethod
    def _decode_simple(value, decode_cb):
        assert isinstance(value, str)
        return value

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expected str after sigil "U"')
        return value

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        assert isinstance(value, str)
        return value

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, str)
        return {cls.sigil: value}


class JoltBytesTransformer(JoltTypeTransformer):
    _supported_types = bytes, bytearray
    sigil = "#"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not (isinstance(value, str)
                or (isinstance(value, list)
                    and all(isinstance(b, int) and 0 <= b <= 255
                            for b in value))):
            raise JOLTValueError("Expected str or list of integers (0-255) "
                                 'after sigil "#"')
        if isinstance(value, str):
            if not re.match(r"^([a-fA-F0-9]{2}\s*)*$", value):
                raise JOLTValueError("Invalid hex encoded string for bytes %s"
                                     % value)
            return bytes.fromhex(value)
        else:
            return bytes(value)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, (bytes, bytearray))
        if not human_readable:
            return {cls.sigil: value.hex().upper()}
        else:
            if sys.version_info >= (3, 8):
                return {cls.sigil: value.hex(" ").upper()}
            else:
                return {cls.sigil: " ".join("{:02X}".format(x) for x in value)}


class JoltDictTransformer(JoltTypeTransformer):
    _supported_types = dict,
    sigil = "{}"

    @staticmethod
    def _decode_simple(value, decode_cb):
        # JOLT does not define a simple dict type. However, for simplicity we
        # allow users to specify simple dicts if the sigil is not in use.
        # E.g., `{"Z": 1}` will be interpreted as an integer (because the sigil
        # `Z` is defined as integer while `{"n": 1}` is interpreted as dict
        # because there is no sigil `n`.
        # Alternatively, if the dict not exactly one key
        # (e.g., `{"Z": 1, "R": "1"}` or `{}`, it can also be unambiguously
        # identified as a dict.
        assert isinstance(value, dict)
        assert all(map(lambda k: isinstance(k, str), value.keys()))
        return {k: decode_cb(v) for k, v in value.items()}

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, dict):
            raise JOLTValueError('Expecting dict after sigil "{}"')
        assert all(map(lambda k: isinstance(k, str), value.keys()))
        return {k: decode_cb(v) for k, v in value.items()}

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        if not all(map(lambda k: isinstance(k, str), value.keys())):
            raise JOLTValueError("Dictionary keys must be strings")
        return {cls.sigil: {k: encode_cb(v) for k, v in value.items()}}


class JoltListTransformer(JoltTypeTransformer):
    _supported_types = list, tuple
    sigil = "[]"

    @staticmethod
    def _decode_simple(value, decode_cb):
        assert isinstance(value, (list, tuple))
        return [decode_cb(v) for v in value]

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list):
            raise JOLTValueError('Expecting list after sigil "[]"')
        return [decode_cb(v) for v in value]

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        assert isinstance(value, (list, tuple))
        return [encode_cb(v) for v in value]

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, (list, tuple))
        return {cls.sigil: [encode_cb(v) for v in value]}


class JoltDateTimeTransformer(JoltTypeTransformer):
    _supported_types = (
        JoltDate, JoltTime, JoltLocalTime,
        JoltDateTime, JoltLocalDateTime, JoltDuration
    )
    sigil = "T"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

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

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        return {cls.sigil: str(value)}


class JoltPointTransformer(JoltTypeTransformer):
    _supported_types = JoltPoint,
    sigil = "@"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError('Expecting str sigil "@"')
        return JoltPoint(value)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        return {cls.sigil: str(value)}


class JoltNodeTransformer(JoltTypeTransformer):
    _supported_types = JoltNode,
    sigil = "()"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list) or len(value) != 3:
            raise JOLTValueError('Expecting list of length 3 after sigil "()"')
        id_, labels, properties = value
        if not isinstance(id_, int):
            raise JOLTValueError("Node id must be int")
        if not isinstance(labels, list):
            raise JOLTValueError("Node labels must be list")
        if not all(map(lambda l: isinstance(l, str), labels)):
            raise JOLTValueError("Node labels must be list of str")
        if not isinstance(properties, dict):
            raise JOLTValueError("Node properties must be dict")
        properties = {k: decode_cb(v) for k, v in properties.items()}
        assert all(map(lambda e: isinstance(e, str), properties.keys()))
        return JoltNode(id_, labels, properties)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: [
            value.id, value.labels,
            {k: encode_cb(v) for k, v in value.properties.items()}
        ]}


class JoltRelationTransformer(JoltTypeTransformer):
    _supported_types = JoltRelationship,
    sigil = "->"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @classmethod
    def _decode_full(cls, value, decode_cb):
        if not isinstance(value, list) or len(value) != 5:
            raise JOLTValueError('Expecting list of length 5 after sigil "%s"'
                                 % cls.sigil)
        id_, start_node_id, rel_type, end_node_id, properties = value
        if not isinstance(id_, int):
            raise JOLTValueError("Relationship id must be int")
        if not isinstance(start_node_id, int):
            raise JOLTValueError("Relationship's start id must be int")
        if not isinstance(rel_type, str):
            raise JOLTValueError("Relationship's type id must be str")
        if not isinstance(end_node_id, int):
            raise JOLTValueError("Relationship's end id must be int")
        if not isinstance(properties, dict):
            raise JOLTValueError("Relationship's properties  must be dict")
        properties = {k: decode_cb(v) for k, v in properties.items()}
        if not all(map(lambda e: isinstance(e, str), properties.keys())):
            raise JOLTValueError("Relationship's properties keys must be str")
        return JoltRelationship(id_, start_node_id, rel_type, end_node_id,
                                properties)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: [
            value.id, value.start_node_id, value.rel_type, value.end_node_id,
            {k: encode_cb(v) for k, v in value.properties.items()}
        ]}


class JoltReverseRelationTransformer(JoltTypeTransformer):
    sigil = "<-"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @classmethod
    def _decode_full(cls, value, decode_cb):
        if not isinstance(value, list) or len(value) != 5:
            raise JOLTValueError('Expecting list of length 5 after sigil "%s"'
                                 % cls.sigil)
        value = [value[idx] for idx in (0, 3, 2, 1, 4)]
        return JoltRelationTransformer.decode_full(value, decode_cb)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        # this class is a pure decoder. encoding always happens with "->" sigil
        raise NotImplementedError

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        # this class is a pure decoder. encoding always happens with "->" sigil
        raise NotImplementedError


class JoltPathTransformer(JoltTypeTransformer):
    _supported_types = JoltPath,
    sigil = ".."

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

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
                if prev_rel.end_node_id != node.id:
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
                if next_rel.start_node_id != node.id:
                    raise JOLTValueError(
                        "Relationship %i did not point to the previous Node "
                        "in the path" % (i + 1)
                    )
        return JoltPath(*path)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: list(map(encode_cb, value.path))}


class Codec:
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

    @classmethod
    def decode(cls, value):
        def transform(value_):
            if isinstance(value_, dict) and len(value_) == 1:
                sigil, content = next(iter(value_.items()))
                match = re.match(r"(.+)(v\d+)", sigil)
                if match:
                    sigil, version = match.groups()
                    other_codec = importlib.import_module(f"..{version}.codec",
                                                          package=__package__)
                    return other_codec.Codec.decode({sigil: content})
                transformer = cls.sigil_to_type.get(sigil)
                if transformer:
                    return transformer.decode_full(value_[sigil], transform)
            transformer = cls.native_to_type.get(type(value_))
            if transformer:
                try:
                    return transformer.decode_simple(value_, transform)
                except NoSimpleRepresentation:
                    pass
            return value_
        return transform(value)

    @classmethod
    def encode_simple(cls, value, human_readable=False):
        def transform(value_):
            transformer = cls.native_to_type.get(type(value_))
            if transformer:
                try:
                    return transformer.encode_simple(
                        value_, transform, human_readable=human_readable
                    )
                except NoSimpleRepresentation:
                    return transformer.encode_full(
                        value_, transform, human_readable=human_readable
                    )
            return value_
        return transform(value)

    @classmethod
    def encode_full(cls, value, human_readable=False):
        def transform(value_):
            transformer = cls.native_to_type.get(type(value_))
            if transformer:
                try:
                    return transformer.encode_full(
                        value_, transform, human_readable=human_readable
                    )
                except NoFullRepresentation:
                    return transformer.encode_simple(
                        value_, transform, human_readable=human_readable
                    )
            return value_
        return transform(value)


decode = Codec.decode
encode_simple = Codec.encode_simple
encode_full = Codec.encode_full


__all__ = [
    Codec,
    decode,
    encode_simple,
    encode_full,
]
