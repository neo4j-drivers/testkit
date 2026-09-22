from __future__ import annotations

import re
import typing as t
from dataclasses import (
    dataclass,
    field,
)

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("LocalTime",)

LOCAL_TIME_RE = re.compile(r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?")


class LocalTime(HttpType):
    @dataclass(frozen=True)
    class SerializationFormat:
        omit_zero_seconds: bool = field(default=False, kw_only=True)
        omit_zero_nanoseconds: bool = field(default=True, kw_only=True)
        trim_nanoseconds: bool = field(default=True, kw_only=True)

    hour: int
    minute: int
    second: int
    nanosecond: int
    fmt: SerializationFormat

    def __init__(
        self,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int,
        fmt: SerializationFormat = SerializationFormat(),  # noqa: B008
    ) -> None:
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.fmt = fmt
        self._validate()

    def _validate(self) -> None:
        if not (0 <= self.hour <= 23):
            raise ValueError("Hour must be in 0..23")
        if not (0 <= self.minute <= 59):
            raise ValueError("Minute must be in 0..59")
        if not (0 <= self.second <= 59):
            raise ValueError("Second must be in 0..59")
        if not (0 <= self.nanosecond <= 999_999_999):
            raise ValueError("Nanosecond must be in 0..999999999")

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("LocalTime", self._str()).serialize()

    def _str(self) -> str:
        if self.nanosecond == 0 and self.fmt.omit_zero_nanoseconds:
            str_time = ""
        else:
            str_time = f".{self.nanosecond:09d}"
        if self.fmt.trim_nanoseconds:
            str_time = str_time.rstrip("0")
            if str_time.endswith("."):
                str_time += "0"

        if self.second != 0 or self.fmt.omit_zero_seconds or str_time:
            str_time = f":{self.second:02d}{str_time}"

        return f"{self.hour:02d}:{self.minute:02d}{str_time}"

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "LocalTime":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        match = LOCAL_TIME_RE.fullmatch(v)
        if match is None:
            value_dict.invalid_value(f"didn't match local time regex: {v!r}")
            return None

        hour = int(match.group(1))
        minute = int(match.group(2))
        second = int(match.group(3) or "0")
        nanosecond = int((match.group(4) or "000000000").ljust(9, "0"))
        return cls(hour, minute, second, nanosecond)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        # Python's native time type does not support nanoseconds
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherTime):
            if value.utc_offset_s is not None:
                return None
            hour = value.hour
            minute = value.minute
            second = value.second
            nanosecond = value.nanosecond
            return cls(hour, minute, second, nanosecond)
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            o = t.cast(LocalTime, other)
            return (
                self.hour == o.hour
                and self.minute == o.minute
                and self.second == o.second
                and self.nanosecond == o.nanosecond
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"
