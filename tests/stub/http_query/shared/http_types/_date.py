from __future__ import annotations

import re
import typing as t
from datetime import date

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Date",)

DATE_RE = re.compile(r"(\d{4}|[+-]\d{4,})-(\d{2})-(\d{2})")


class Date(HttpType):
    year: int
    month: int
    day: int

    def __init__(self, year: int, month: int, day: int) -> None:
        self.year = year
        self.month = month
        self.day = day
        self._validate()

    def _validate(self) -> None:
        year_mod = (abs(self.year) - 1) % 400
        try:
            date(year_mod + 1, self.month, self.day)
        except ValueError as exc:
            raise ValueError(
                f"Invalid date: {self._str()}"
                "(note: the year in the attached exception might be a lie...)"
            ) from exc

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Date", self._str()).serialize()

    def _str(self) -> str:
        if self.year > 9999:
            return f"+{self.year}-{self.month:02d}-{self.day:02d}"
        elif self.year < 0:
            return f"{self.year:05d}-{self.month:02d}-{self.day:02d}"
        else:
            return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Date":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        match = DATE_RE.fullmatch(v)
        if match is None:
            value_dict.invalid_value(f"didn't match date regex: {v!r}")
            return None

        year, month, day = map(int, match.groups())
        return cls(year, month, day)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if isinstance(value, date):
            return cls(value.year, value.month, value.day)
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherDate):
            year, month, day = value.year, value.month, value.day
            return cls(year, month, day)
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            o = t.cast(Date, other)
            return (
                self.year == o.year
                and self.month == o.month
                and self.day == o.day
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"
