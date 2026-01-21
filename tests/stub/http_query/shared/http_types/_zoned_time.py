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

__all__ = ("ZonedTime",)

ZONED_TIME_RE = re.compile(
    r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?"
    r"([+-]\d{4}|[+-]\d{2}:\d{2}(?::\d{2})?|Z)"
)


class ZonedTime(HttpType):
    @dataclass(frozen=True)
    class SerializationFormat:
        use_zulu: bool = field(default=True, kw_only=True)
        omit_zero_seconds: bool = field(default=False, kw_only=True)
        omit_zero_nanoseconds: bool = field(default=True, kw_only=True)
        trim_nanoseconds: bool = field(default=True, kw_only=True)

    hour: int
    minute: int
    second: int
    nanosecond: int
    utc_offset_s: int
    fmt: SerializationFormat

    def __init__(
        self,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int,
        utc_offset_s: int,
        fmt: SerializationFormat = SerializationFormat(),  # noqa: B008
    ) -> None:
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.utc_offset_s = utc_offset_s
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
        if not (-86400 <= self.utc_offset_s <= 86400):
            raise ValueError("UTC offset seconds must be in -86400..86400")

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Time", self._str()).serialize()

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

        str_time = f"{self.hour:02d}:{self.minute:02d}{str_time}"

        if self.utc_offset_s == 0 and self.fmt.use_zulu:
            str_time += "Z"
        else:
            offset_minutes, offset_seconds = divmod(abs(self.utc_offset_s), 60)
            offset_hours, offset_minutes = divmod(offset_minutes, 60)
            sign = "+" if self.utc_offset_s >= 0 else "-"
            str_time += f"{sign}{offset_hours:02d}:{offset_minutes:02d}"
            if offset_seconds:
                str_time += f":{offset_seconds:02d}"

        return str_time

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Time":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        match = ZONED_TIME_RE.fullmatch(v)
        if match is None:
            value_dict.invalid_value(f"didn't match zoned time regex: {v!r}")
            return None

        hour = int(match.group(1))
        minute = int(match.group(2))
        second = int(match.group(3) or "0")
        nanosecond = int((match.group(4) or "000000000").ljust(9, "0"))
        offset = cls._parse_offset(match.group(5))
        return cls(hour, minute, second, nanosecond, offset)

    @classmethod
    def _parse_offset(cls, offset_str: str) -> int:
        if offset_str == "Z":
            return 0
        sign = 1 if offset_str[0] == "+" else -1
        offset_str = offset_str[1:]

        if ":" not in offset_str:
            return int(offset_str) * sign * 3600

        parts = list(map(int, offset_str.split(":")))
        if len(parts) < 3:
            parts.append(0)
        hours, minutes, seconds = parts
        return sign * hours * 3600 + minutes * 60 + seconds

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        # Python's native time type does not support time zones/offsets
        # neither does it support nanoseconds
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherTime):
            if value.utc_offset_s is None:
                return None
            hour = value.hour
            minute = value.minute
            second = value.second
            nanosecond = value.nanosecond
            utc_offset_s = value.utc_offset_s
            return cls(hour, minute, second, nanosecond, utc_offset_s)
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            o = t.cast(ZonedTime, other)
            return (
                self.hour == o.hour
                and self.minute == o.minute
                and self.second == o.second
                and self.nanosecond == o.nanosecond
                and self.utc_offset_s == o.utc_offset_s
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"
