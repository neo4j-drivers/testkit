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

__all__ = ("Duration",)

# Notes:
#  * The real server does not accept more than 9 decimal places for seconds.
#    I.e., "PT0.000000001" is fine, "PT0.0000000001" isn't.
#    This regex mirrors this.
#  * While the server does accept float values for all components, this regex
#    does so only for seconds.
#    This is because float values are (depending on the component)
#    * suffering from imprecision
#    * yielding unexpected results (e.g, month fractions expressed as seconds)
#    * not necessary to represent the full range of supported values
#    Therefore, we don't want drivers to ever use them.
#  * ISO 8601 does not allow mixing weeks ("PnW") with other components.
#    However, the real server does. So do we.
#  * The server does not care about capitalization (e.g., "p1y" == "P1Y").
#    However, to stick closer to the ISO standard, the regex enforces capital
#    letters.
#  * The server does accept alternative formats
#    * "P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss.sssssssss]"
#    * "P[YYYY][MM][DD]T[hh][mm][ss.sssssssss]"
#    * Some short-forms of these like "P[YYYY][MM][DD]"
#    This regex does not, simply for all our sanity's sake.
#    Note that these formats are limited in that it's non-trivial to encode
#    negative components.
DURATION_RE = re.compile(
    r"P"
    r"(?:([+-]?\d+)Y)?"
    r"(?:([+-]?\d+)M)?"
    r"(?:([+-]?\d+)W)?"
    r"(?:([+-]?\d+)D)?"
    r"(T"
    r"(?:([+-]?\d+)H)?"
    r"(?:([+-]?\d+)M)?"
    r"(?:([+-]?\d+)(?:[.,](\d{1,9}))?S)?"
    r")?"
)


class Duration(HttpType):
    @dataclass(frozen=True)
    class SerializationFormat:
        omit_zero_years: bool = field(default=True, kw_only=True)
        omit_zero_months: bool = field(default=True, kw_only=True)
        omit_zero_weeks: bool = field(default=True, kw_only=True)
        omit_zero_days: bool = field(default=True, kw_only=True)
        omit_zero_hours: bool = field(default=True, kw_only=True)
        omit_zero_minutes: bool = field(default=True, kw_only=True)
        omit_zero_seconds: bool = field(default=True, kw_only=True)
        omit_zero_nanoseconds: bool = field(default=True, kw_only=True)
        trim_nanoseconds: bool = field(default=True, kw_only=True)
        decimal_point: str = field(default=".", kw_only=True)

    years: int
    months: int
    weeks: int
    days: int
    hours: int
    minutes: int
    seconds: int
    nanoseconds: int
    fmt: SerializationFormat

    def __init__(
        self,
        years: int,
        months: int,
        weeks: int,
        days: int,
        hours: int,
        minutes: int,
        seconds: int,
        nanoseconds: int,
        fmt: SerializationFormat = SerializationFormat(),  # noqa: B008
    ) -> None:
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.nanoseconds = nanoseconds
        self.fmt = fmt
        self._validate()

    def _validate(self) -> None:
        if (
            self.nanoseconds != 0
            and self.seconds != 0
            and (self.nanoseconds > 0) != (self.seconds > 0)
        ):
            raise ValueError("nanoseconds and seconds must have the same sign")
        if not (-1_000_000_000 < self.nanoseconds < 1_000_000_000):
            raise ValueError(
                "nanoseconds must be in range -999,999,999..999,999,999"
            )

    def normalized_distributed(self) -> te.Self:
        # Normalize such that most components are non-0 and that each
        # component's value is minimal
        #
        # Note: the weeks component will be 0 anyway, because according to ISO
        #       mixing weeks with other components is not valid
        compact = self.normalize_compact()
        assert (
            compact.years
            == compact.weeks
            == compact.hours
            == compact.minutes
            == 0
        )

        year, month = symmetric_divmod(compact.months, 12)

        minute, second = symmetric_divmod(compact.seconds, 60)
        hour, minute = symmetric_divmod(minute, 60)

        return type(self)(
            years=year,
            months=month,
            weeks=0,
            days=compact.days,
            hours=hour,
            minutes=minute,
            seconds=second,
            nanoseconds=compact.nanoseconds,
            fmt=self.fmt,
        )

    def normalize_compact(self) -> te.Self:
        # Normalize such that only months, days, seconds, and nanoseconds are
        # non-zero
        months = self.years * 12 + self.months
        days = self.weeks * 7 + self.days
        seconds = self.hours * 3600 + self.minutes * 60 + self.seconds
        nanoseconds = self.nanoseconds
        if seconds > 0 and nanoseconds < 0:
            seconds -= 1
            nanoseconds += 1_000_000_000
        elif seconds < 0 and nanoseconds > 0:
            seconds += 1
            nanoseconds -= 1_000_000_000
        return type(self)(
            years=0,
            months=months,
            weeks=0,
            days=days,
            hours=0,
            minutes=0,
            seconds=seconds,
            nanoseconds=nanoseconds,
            fmt=self.fmt,
        )

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Duration", self._str()).serialize()

    def _str(self) -> str:
        str_date = ""
        if self.years != 0 or not self.fmt.omit_zero_years:
            str_date += f"{self.years}Y"
        if self.months != 0 or not self.fmt.omit_zero_months:
            str_date += f"{self.months}M"
        if self.weeks != 0 or not self.fmt.omit_zero_weeks:
            str_date += f"{self.weeks}W"
        if self.days != 0 or not self.fmt.omit_zero_days:
            str_date += f"{self.days}D"

        str_time = ""
        if self.hours != 0 or not self.fmt.omit_zero_hours:
            str_time += f"{self.hours}H"
        if self.minutes != 0 or not self.fmt.omit_zero_minutes:
            str_time += f"{self.minutes}M"

        if self.nanoseconds == 0 and self.fmt.omit_zero_nanoseconds:
            str_ns = ""
        else:
            str_ns = f"{self.fmt.decimal_point}{abs(self.nanoseconds):09d}"
        if self.fmt.trim_nanoseconds:
            str_ns = str_ns.rstrip("0")
            if str_ns == self.fmt.decimal_point:
                str_ns += "0"

        if self.seconds != 0 or not self.fmt.omit_zero_seconds or str_ns:
            if self.seconds < 0 or self.nanoseconds < 0:
                str_time += f"-{abs(self.seconds)}{str_ns}S"
            else:
                str_time += f"{abs(self.seconds)}{str_ns}S"

        if str_time:
            str_time = f"T{str_time}"
        elif not str_date:
            str_time = "T0S"

        return f"P{str_date}{str_time}"

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Duration":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        match = DURATION_RE.fullmatch(v)
        if match is None:
            value_dict.invalid_value(f"didn't match duration regex: {v!r}")
            return None

        years = int(match.group(1) or 0)
        months = int(match.group(2) or 0)
        weeks = int(match.group(3) or 0)
        days = int(match.group(4) or 0)

        with_time = match.group(5) is not None
        with_hours = match.group(6) is not None
        hours = int(match.group(6) or 0)
        with_minutes = match.group(7) is not None
        minutes = int(match.group(7) or 0)
        with_seconds = match.group(8) is not None
        seconds = int(match.group(8) or 0)
        nanoseconds = int((match.group(9) or "000000000").ljust(9, "0"))
        if match.group(8) and match.group(8).startswith("-"):
            nanoseconds *= -1

        if with_time and not (with_hours or with_minutes or with_seconds):
            value_dict.invalid_value(f"duration with empty time: {v!r}")
            return None

        return cls(
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            nanoseconds=nanoseconds,
        )

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherDuration):
            months = value.months
            days = value.days
            seconds = value.seconds
            nanoseconds = value.nanoseconds
            if seconds < 0 and nanoseconds > 0:
                seconds += 1
                nanoseconds -= 1_000_000_000
            elif seconds > 0 and nanoseconds < 0:
                seconds -= 1
                nanoseconds += 1_000_000_000

            return cls(
                years=0,
                months=months,
                weeks=0,
                days=days,
                hours=0,
                minutes=0,
                seconds=seconds,
                nanoseconds=nanoseconds,
            )
        return None

    def __eq__(self, other: object, /) -> bool:
        if type(self) is type(other):
            s = self.normalize_compact()
            o = t.cast(Duration, other).normalize_compact()
            return (
                s.months == o.months
                and s.days == o.days
                and s.seconds == o.seconds
                and s.nanoseconds == o.nanoseconds
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"


def symmetric_divmod(a: int, b: int, /) -> tuple[int, int]:
    """Like divmod, but the remainder has the same sign as the dividend."""
    q, r = divmod(a, b)
    if r != 0 and (r < 0) != (b < 0):
        q -= 1
        r += b
    return q, r
