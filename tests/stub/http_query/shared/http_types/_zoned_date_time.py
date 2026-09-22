from __future__ import annotations

import typing as t

import typing_extensions as te

from ._base import (
    HttpType,
    ValueDict,
)
from ._base_date_time import BaseDateTime
from ._protocol_version import ProtocolVersion

__all__ = ("ZonedDateTime",)


class ZonedDateTime(HttpType):
    SerializationFormat: te.TypeAlias = BaseDateTime.SerializationFormat

    _base: BaseDateTime

    def __init__(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int,
        utc_offset_s: int,
        zone_id: str | None = None,
        fmt: SerializationFormat = SerializationFormat(),  # noqa: B008
    ) -> None:
        self._base = BaseDateTime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            utc_offset_s,
            zone_id,
            fmt,
        )

    @classmethod
    def _from_base(cls, base: BaseDateTime | None) -> te.Self | None:
        if base is None or base.utc_offset_s is None:
            return None
        obj = cls.__new__(cls)
        obj._base = base
        return obj

    @property
    def year(self) -> int:
        return self._base.year

    @year.setter
    def year(self, value: int) -> None:
        self._base.year = value

    @property
    def month(self) -> int:
        return self._base.month

    @month.setter
    def month(self, value: int) -> None:
        self._base.month = value

    @property
    def day(self) -> int:
        return self._base.day

    @day.setter
    def day(self, value: int) -> None:
        self._base.day = value

    @property
    def hour(self) -> int:
        return self._base.hour

    @hour.setter
    def hour(self, value: int) -> None:
        self._base.hour = value

    @property
    def minute(self) -> int:
        return self._base.minute

    @minute.setter
    def minute(self, value: int) -> None:
        self._base.minute = value

    @property
    def second(self) -> int:
        return self._base.second

    @second.setter
    def second(self, value: int) -> None:
        self._base.second = value

    @property
    def nanosecond(self) -> int:
        return self._base.nanosecond

    @nanosecond.setter
    def nanosecond(self, value: int) -> None:
        self._base.nanosecond = value

    @property
    def utc_offset_s(self) -> int:
        utc_offset_s = self._base.utc_offset_s
        assert utc_offset_s is not None
        return utc_offset_s

    @utc_offset_s.setter
    def utc_offset_s(self, value: int) -> None:
        self._base.utc_offset_s = value

    @property
    def zone_id(self) -> str | None:
        return self._base.zone_id

    @zone_id.setter
    def zone_id(self, value: str | None) -> None:
        self._base.zone_id = value

    @property
    def fmt(self) -> SerializationFormat:
        return self._base.fmt

    @fmt.setter
    def fmt(self, value: SerializationFormat) -> None:
        self._base.fmt = value

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("ZonedDateTime", str(self._base)).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "ZonedDateTime":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        try:
            base = BaseDateTime.deserialize(value_dict.value)
        except ValueError as e:
            value_dict.invalid_value(
                f"invalid zoned date time string {v!r}: {e}"
            )
            return None

        obj = cls._from_base(base)
        if obj is None:
            value_dict.invalid_value("missing time zone information")

        return cls._from_base(base)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        # Not supported: Python's native datetime does not support nanoseconds
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        base = BaseDateTime.from_cypher_type(value)
        return cls._from_base(base)

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            o = t.cast(ZonedDateTime, other)
            return self._base.equals(o._base, normalize=True)
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._base}>"
