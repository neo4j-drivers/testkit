from __future__ import annotations

import re
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timedelta,
)

import typing_extensions as te

from nutkit import protocol as types

__all__ = (
    "DeserializeError",
    "BaseDateTime",
)

DATETIME_RE = re.compile(
    r"(\d{4}|[+-]\d{4,})-(\d{2})-(\d{2})T"
    r"(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?"
    r"([+-]\d{4}|[+-]\d{2}:\d{2}(?::\d{2})?|Z)?"
    r"(?:\[(.*?)\])?"
)


class DeserializeError(ValueError):
    pass


class BaseDateTime:
    @dataclass(frozen=True)
    class SerializationFormat:
        use_zulu: bool = field(default=True, kw_only=True)
        omit_zero_seconds: bool = field(default=False, kw_only=True)
        omit_zero_nanoseconds: bool = field(default=True, kw_only=True)
        trim_nanoseconds: bool = field(default=True, kw_only=True)

    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    nanosecond: int
    utc_offset_s: int | None
    zone_id: str | None
    fmt: SerializationFormat

    def __init__(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int,
        utc_offset_s: int | None,
        zone_id: str | None = None,
        fmt: SerializationFormat = SerializationFormat(),  # noqa: B008
    ) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.utc_offset_s = utc_offset_s
        self.zone_id = zone_id
        self.fmt = fmt
        self._validate()

    def _validate(self) -> None:
        if self.zone_id is not None and self.utc_offset_s is None:
            raise ValueError(
                "zone_id may only be present when an utc offset is provided"
            )
        try:
            self._normalize_to_utc()
        except ValueError as exc:
            raise ValueError(
                f"Invalid date time: {self}"
                "(note: the year in the attached exception might be a lie...)"
            ) from exc

    @classmethod
    def deserialize(
        cls,
        value: str,
    ) -> te.Self:
        match = DATETIME_RE.fullmatch(value)
        if match is None:
            raise DeserializeError("didn't match date time regex")

        try:
            year = int(match.group(1))
        except Exception as e:
            raise DeserializeError(f"invalid year: {e!r}") from None
        try:
            month = int(match.group(2))
        except Exception as e:
            raise DeserializeError(f"invalid month: {e!r}") from None
        try:
            day = int(match.group(3))
        except Exception as e:
            raise DeserializeError(f"invalid day: {e!r}") from None

        hour = int(match.group(4))
        minute = int(match.group(5))
        second = int(match.group(6) or "0")
        nanosecond = int((match.group(7) or "000000000").ljust(9, "0"))
        offset_group = match.group(8)
        zone = match.group(9)

        if offset_group is None:
            if zone is not None:
                raise DeserializeError(
                    "must not specify zone id without offset"
                )
            offset = None
        else:
            offset = cls._parse_offset(offset_group)

        return cls(
            year, month, day, hour, minute, second, nanosecond, offset, zone
        )

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
    def from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherDateTime):
            year = value.year
            month = value.month
            day = value.day
            hour = value.hour
            minute = value.minute
            second = value.second
            nanosecond = value.nanosecond
            utc_offset_s = value.utc_offset_s
            zone_id = value.timezone_id

            return cls(
                int(year),
                int(month),
                int(day),
                int(hour),
                int(minute),
                int(second),
                int(nanosecond),
                utc_offset_s,
                zone_id,
            )
        return None

    def _normalize_to_utc(self) -> te.Self:
        # Python's datetime only supports years in range 1..9999.
        # Therefore, we need to map the year into that range for the purpose.
        # The leap year rules of the Gregorian calendar repeat every 400 years.
        if not self.utc_offset_s:
            return self
        year_sign = 1 if self.year >= 0 else -1
        year_abs = abs(self.year)
        year_div, year_mod = divmod(year_abs - 1, 400)
        dt = datetime(
            year=year_mod + 401,
            month=self.month,
            day=self.day,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.nanosecond // 1000,
        )
        dt_utc = dt - timedelta(seconds=self.utc_offset_s)
        new_year = year_sign * ((year_div * 400 + dt_utc.year - 401) + 1)
        return type(self)(
            year=new_year,
            month=dt_utc.month,
            day=dt_utc.day,
            hour=dt_utc.hour,
            minute=dt_utc.minute,
            second=dt_utc.second,
            nanosecond=self.nanosecond,
            utc_offset_s=0,
            zone_id=self.zone_id,
            fmt=self.fmt,
        )

    def equals(self, other: BaseDateTime, *, normalize: bool) -> bool:
        s, o = self, other
        if self.zone_id != o.zone_id or self.nanosecond != o.nanosecond:
            return False
        if normalize:
            s = s._normalize_to_utc()
            o = o._normalize_to_utc()
        return (
            s.year == o.year
            and s.month == o.month
            and s.day == o.day
            and s.hour == o.hour
            and s.minute == o.minute
            and s.second == o.second
        )

    def __str__(self) -> str:
        str_date = f"{self.year:04d}-{self.month:02d}-{self.day:02d}"
        if self.nanosecond == 0 and self.fmt.omit_zero_nanoseconds:
            str_frac = ""
        else:
            str_frac = f".{self.nanosecond:09d}"
        if self.fmt.trim_nanoseconds:
            str_frac = str_frac.rstrip("0")
            if str_frac.endswith("."):
                str_frac += "0"

        str_time = f"{self.hour:02d}:{self.minute:02d}"
        if self.second != 0 or self.fmt.omit_zero_seconds or str_frac:
            str_time = f"{str_time}:{self.second:02d}{str_frac}"

        str_date_time = f"{str_date}T{str_time}"
        if self.utc_offset_s is None:
            return str_date_time

        if self.utc_offset_s == 0 and self.fmt.use_zulu:
            str_offset = "Z"
        else:
            offset_minutes, offset_seconds = divmod(abs(self.utc_offset_s), 60)
            offset_hours, offset_minutes = divmod(offset_minutes, 60)
            sign = "+" if self.utc_offset_s >= 0 else "-"
            str_offset = f"{sign}{offset_hours:02d}:{offset_minutes:02d}"
            if offset_seconds:
                str_offset += f":{offset_seconds:02d}"

        if self.zone_id is not None:
            str_zone = f"[{self.zone_id}]"
        else:
            str_zone = ""

        return f"{str_date_time}{str_offset}{str_zone}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self}>"
