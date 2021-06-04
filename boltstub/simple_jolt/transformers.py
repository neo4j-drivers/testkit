import abc
import inspect
import re
import sys

from .errors import (
    NoSimpleRepresentation,
    NoFullRepresentation
)
from .types import (
    JoltDate,
    JoltTime,
    JoltLocalTime,
    JoltDateTime,
    JoltLocalDateTime,
    JoltDuration,
    JoltPoint,
    JoltNode,
    JoltRelationship,
    JoltPath,
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
        assert isinstance(value, bool)
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
        assert isinstance(value, str)
        value = re.sub(r"\.\d*$", "", value)
        return int(value)

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
        assert isinstance(value, str)
        return float(value)

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
        assert isinstance(value, str)
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
        assert isinstance(value, (str, list))
        if isinstance(value, str):
            assert re.match(r"^([a-fA-F0-9]{2}\s*)+$", value)
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
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        assert isinstance(value, dict)
        if not all(map(lambda k: isinstance(k, str), value.keys())):
            raise ValueError("Dictionary keys must be strings")
        return {k: decode_cb(v) for k, v in value.items()}

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        if not all(map(lambda k: isinstance(k, str), value.keys())):
            raise ValueError("Dictionary keys must be strings")
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
        assert isinstance(value, (list, tuple))
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
        raise ValueError("Couldn't parse {} as any of {}".format(value, clss))

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
        assert isinstance(value, str)
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
        id_, labels, properties = value
        assert isinstance(id_, int)
        assert isinstance(labels, list)
        assert all(map(lambda l: isinstance(l, str), labels))
        assert isinstance(properties, dict)
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

    @staticmethod
    def _decode_full(value, decode_cb):
        id_, start_node_id, rel_type, end_node_id, properties = value
        assert isinstance(id_, int)
        assert isinstance(start_node_id, int)
        assert isinstance(rel_type, str)
        assert isinstance(end_node_id, int)
        assert isinstance(properties, dict)
        properties = {k: decode_cb(v) for k, v in properties.items()}
        assert all(map(lambda e: isinstance(e, str), properties.keys()))
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

    @staticmethod
    def _decode_full(value, decode_cb):
        value = [value[idx] for idx in (0, 3, 2, 1, 4)]
        return JoltRelationTransformer.decode_full(value, decode_cb)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        # this class is a pure decoder. encoding always happens with "->" sigil
        raise NotImplemented

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        # this class is a pure decoder. encoding always happens with "->" sigil
        raise NotImplemented


class JoltPathTransformer(JoltTypeTransformer):
    _supported_types = JoltPath,
    sigil = ".."

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        path = list(map(decode_cb, value))
        assert not path or len(path) % 2 == 1
        for i in range(0, len(path), 2):
            node = path[i]
            assert isinstance(node, JoltNode)
            if i != 0:
                prev_rel = path[i - 1]
                assert isinstance(prev_rel, JoltRelationship)
                assert prev_rel.end_node_id == node.id
            if i < len(path) - 1:
                next_rel = path[i + 1]
                assert isinstance(next_rel, JoltRelationship)
                assert next_rel.start_node_id == node.id
        return JoltPath(*path)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        assert isinstance(value, cls._supported_types)
        return {cls.sigil: list(map(encode_cb, value.path))}


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


def decode(value):
    def transform(value_):
        if isinstance(value_, dict) and len(value_) == 1:
            sigil = next(iter(value_))
            transformer = sigil_to_type.get(sigil)
            if transformer:
                return transformer.decode_full(value_[sigil], transform)
        transformer = native_to_type.get(type(value_))
        if transformer:
            try:
                return transformer.decode_simple(value_, transform)
            except NoSimpleRepresentation:
                pass
        return value_
    return transform(value)


def encode_simple(value, human_readable=False):
    def transform(value_):
        transformer = native_to_type.get(type(value_))
        if transformer:
            try:
                return transformer.encode_simple(value_, transform,
                                                 human_readable=human_readable)
            except NoSimpleRepresentation:
                return transformer.encode_full(value_, transform,
                                               human_readable=human_readable)
        return value_
    return transform(value)


def encode_full(value, human_readable=False):
    def transform(value_):
        transformer = native_to_type.get(type(value_))
        if transformer:
            try:
                return transformer.encode_full(value_, transform,
                                               human_readable=human_readable)
            except NoFullRepresentation:
                return transformer.encode_simple(value_, transform,
                                                 human_readable=human_readable)
        return value_
    return transform(value)


__all__ = [
    # JoltNullTransformer,
    # JoltBoolTransformer,
    # JoltIntTransformer,
    # JoltFloatTransformer,
    # JoltStrTransformer,
    # JoltBytesTransformer,
    # JoltDictTransformer,
    # JoltListTransformer,
    # JoltDateTimeTransformer,
    # JoltPointTransformer,
    # JoltNodeTransformer,
    # JoltRelationTransformer,
    # JoltPathTransformer,
    decode,
    encode_full,
    encode_simple,
]
