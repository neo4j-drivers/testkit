import abc
import inspect
import numpy as np
import datetime
import re
import sys


class NoSimpleRepresentation(ValueError):
    pass


class NoFullRepresentation(ValueError):
    pass


class JoltType(abc.ABC):
    _supported_native_types = ()
    sigil = None

    # def __init__(self, native):
    #     self._native = native
    #
    # def __new__(cls, native):
    #     return cls.from_native(native)
    #
    # @abc.abstractmethod
    # def to_native(self):
    #     pass
    #
    # def to_native_recursive(self):
    #     return self.to_native()
    #
    # @classmethod
    # def from_native(cls, native):
    #     if not isinstance(native, cls._supported_native_types):
    #         return TypeError(
    #             "Can not convert {} to {}. Supported types: {}".format(
    #                 native.__class__.__name__,
    #                 cls.__name__,
    #                 ", ".join(map(lambda t: t.__name__,
    #                               cls._supported_native_types))
    #             )
    #         )
    #     obj = object.__new__(cls)
    #     obj.__init__(native)

    @staticmethod
    @abc.abstractmethod
    def decode_simple(value):
        pass

    @staticmethod
    @abc.abstractmethod
    def decode_full(value):
        pass

    @staticmethod
    @abc.abstractmethod
    def encode_simple(value):
        pass

    @staticmethod
    @abc.abstractmethod
    def encode_full(value, human_readable=False):
        pass


class JoltNull(JoltType):
    _supported_native_types = type(None),

    @staticmethod
    @abc.abstractmethod
    def decode_simple(value):
        assert value is None
        return None

    @staticmethod
    @abc.abstractmethod
    def decode_full(value):
        # has not full representation
        raise NoFullRepresentation()

    @staticmethod
    def encode_simple(value):
        assert value is None
        return None

    @staticmethod
    def encode_full(value, human_readable=False):
        # has not full representation
        raise NoFullRepresentation()


class JoltBool(JoltType):
    _supported_native_types = bool,
    sigil = "?"

    @staticmethod
    def decode_simple(value):
        assert isinstance(value, bool)
        return value

    @staticmethod
    def decode_full(value):
        assert isinstance(value, bool)
        return value

    @staticmethod
    def encode_simple(value):
        assert isinstance(value, bool)
        return value

    @classmethod
    def encode_full(cls, value, human_readable=False):
        assert isinstance(value, bool)
        return {cls.sigil: value}


class JoltInt(JoltType):
    _supported_native_types = int,
    sigil = "Z"

    @staticmethod
    def decode_simple(value):
        assert isinstance(value, int)
        if -0x80000000 <= value < 0x80000000:
            return value
        else:
            return float(value)

    @staticmethod
    def decode_full(value):
        assert isinstance(value, str)
        return int(float(value))

    @staticmethod
    def encode_simple(value):
        assert isinstance(value, int)
        if -0x80000000 <= value < 0x80000000:
            return value
        else:
            raise NoSimpleRepresentation()

    @classmethod
    def encode_full(cls, value, human_readable=False):
        assert isinstance(value, int)
        return {cls.sigil: str(value)}


class JoltFloat(JoltType):
    _supported_native_types = float,
    sigil = "R"

    _constant_translation = {
        "inf": "+Infinity",
        "-inf": "-Infinity",
        "nan": "NaN",
    }

    @staticmethod
    def decode_simple(value):
        assert isinstance(value, float)
        return float

    @staticmethod
    def decode_full(value):
        assert isinstance(value, str)
        return float(value)

    @staticmethod
    def encode_simple(value):
        assert isinstance(value, float)
        return value

    @classmethod
    def encode_full(cls, value, human_readable=False):
        assert isinstance(value, float)
        str_repr = str(float)
        return {"R": cls._constant_translation.get(str_repr, str_repr)}


class JoltStr(JoltType):
    _supported_native_types = str,
    sigil = "U"

    @staticmethod
    def decode_simple(value):
        assert isinstance(value, str)
        return value

    @staticmethod
    def decode_full(value):
        assert isinstance(value, str)
        return value

    @staticmethod
    def encode_simple(value):
        assert isinstance(value, str)
        return value

    @classmethod
    def encode_full(cls, value, human_readable=False):
        assert isinstance(value, str)
        return {cls.sigil: value}


class JoltBytes(JoltType):
    _supported_native_types = bytes, bytearray
    sigil = "#"

    @staticmethod
    def decode_simple(value):
        raise NoSimpleRepresentation()

    @staticmethod
    def decode_full(value):
        assert isinstance(value, (str, list))
        if isinstance(value, str):
            assert re.match(r"^([a-fA-F0-9]{2}\s*)+$", value)
            return bytes.fromhex(value)
        else:
            return bytes(value)

    @staticmethod
    def encode_simple(value):
        raise NoSimpleRepresentation()

    @classmethod
    def encode_full(cls, value, human_readable=False):
        assert isinstance(value, (bytes, bytearray))
        if not human_readable:
            return {cls.sigil: value.hex().upper()}
        else:
            if sys.version_info >= (3, 8):
                return {cls.sigil: value.hex(" ").upper()}
            else:
                return {cls.sigil: " ".join("{:02X}".format(x) for x in value)}


class JoltDict(JoltType):
    _supported_native_types = dict,
    sigil = "{}"

    @staticmethod
    def decode_simple(value):
        raise NoSimpleRepresentation()

    @staticmethod
    def decode_full(value):
        assert isinstance(value, dict)
        return value

    @staticmethod
    def encode_simple(value):
        raise NoSimpleRepresentation()

    @classmethod
    def encode_full(cls, value, human_readable=False):
        return {cls.sigil: value}


class JoltDateWrapper(np.datetime64):
    pass


class JoltTimeWrapper(np.datetime64):
    pass


class JoltLocalTimeWrapper(np.datetime64):
    pass


class JoltDateTimeWrapper(np.datetime64):
    pass


class JoltLocalDateTimeWrapper(np.datetime64):
    pass


class JoltDurationWrapper(np.timedelta64):
    pass


class JoltDateTime(JoltType):
    _supported_native_types = (
        datetime.datetime, datetime.timedelta, datetime.time, datetime.date,
        JoltDateWrapper, JoltTimeWrapper, JoltLocalTimeWrapper,
        JoltDateTimeWrapper, JoltLocalDateTimeWrapper, JoltDurationWrapper
    )
    sigil = "T"

    @staticmethod
    def decode_simple(value):
        raise NoSimpleRepresentation()

    @staticmethod
    def decode_full(value):
        assert isinstance(value, str)
        return value

    @staticmethod
    def encode_simple(value):
        raise NoSimpleRepresentation()

    @classmethod
    def encode_full(cls, value, human_readable=False):
        return {cls.sigil: value}

sigil_to_type = {
    cls.sigil: cls
    for name, cls in inspect.getmembers(globals())
    if (inspect.isclass(cls) and issubclass(cls, JoltType)
        and cls.sigil is not None)
}
native_to_type = {
    cls.sigil: cls
    for name, cls in inspect.getmembers(globals())
    if (inspect.isclass(cls) and issubclass(cls, JoltType)
        and cls.sigil is not None)
}


__all__ = []


if __name__ == "__main__":
    print(JoltNull.from_native(None))
