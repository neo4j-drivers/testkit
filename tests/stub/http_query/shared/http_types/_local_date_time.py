from __future__ import annotations

import typing as t

import typing_extensions as te

from ._base import (
    HttpType,
    ValueDict,
)
from ._base_date_time import (
    BaseDateTime,
    DeserializeError,
)
from ._protocol_version import ProtocolVersion

__all__ = ("LocalDateTime",)


class LocalDateTime(HttpType):
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
            None,
            None,
            fmt,
        )

    @classmethod
    def _from_base(cls, base: BaseDateTime | None) -> te.Self | None:
        if (
            base is None
            or base.zone_id is not None
            or base.utc_offset_s is not None
        ):
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
    def fmt(self) -> SerializationFormat:
        return self._base.fmt

    @fmt.setter
    def fmt(self, value: SerializationFormat) -> None:
        self._base.fmt = value

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("LocalDateTime", str(self._base)).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "LocalDateTime":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        try:
            base = BaseDateTime.deserialize(v)
        except DeserializeError as e:
            value_dict.invalid_value(f"invalid date time string {v!r}: {e}")
            return None

        obj = cls._from_base(base)
        if obj is None:
            value_dict.invalid_value(
                f"must not contain time zone information: {v!r}"
            )
        return obj

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
            o = t.cast(LocalDateTime, other)
            return self._base.equals(o._base, normalize=False)
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._base}>"
