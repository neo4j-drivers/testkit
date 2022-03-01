import abc
import datetime
import re

from .errors import JOLTValueError


class JoltType:
    pass


class _JoltParsedType(JoltType, abc.ABC):
    # to be overridden in subclasses (this re never matches)
    _parse_re = re.compile(r"^(?= )$")

    def __init__(self, value: str):
        match = self._parse_re.match(value)
        if not match:
            raise JOLTValueError(
                "{} didn't match the types format: {}".format(
                    value, self._parse_re
                )
            )
        self._str = value
        self._groups = match.groups()

    def __str__(self):
        return self._str


class JoltDate(_JoltParsedType):
    _parse_re = re.compile(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$")
    # yes:
    # 2020-01-01
    # 2020-01
    # 2020

    # no:
    # 2020-1-1
    # --1

    def __init__(self, value: str):
        def parse_group(g):
            if g is None:
                return 1
            return int(g)

        super().__init__(value)
        years, months, days = map(parse_group, self._groups)
        dt = datetime.date(years, months, days)
        epoch = datetime.date(1970, 1, 1)
        self.days = (dt - epoch).days

    @classmethod
    def new(cls, days: int):
        epoch = datetime.date(1970, 1, 1)
        delta = datetime.timedelta(days=days)
        return cls(str(epoch + delta))

    def __eq__(self, other):
        if not isinstance(other, JoltDate):
            return NotImplemented
        return self.days == other.days

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.days)


class JoltTime(_JoltParsedType):
    _parse_re = re.compile(
        r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?"
        r"(Z|\+00|[+-]00(?::?[0-5][0-9]|60)|"
        r"(?:[+-](?:0[1-9]|1[0-9]|2[0-3]))(?::?[0-5][0-9]|60)?)$"
    )
    # yes:
    # 12:00:00.000000000+0000
    # 12:00:00.000+0000
    # 12:00:00+00:00
    # 12:00:00+00
    # 12:00:00Z
    # 12:00Z
    # 12Z
    # 12:00:00-01

    # no:
    # 12:00:00.0000000000Z
    # 12:00:00-0000
    # 12:0:0Z

    def __init__(self, value: str):
        def parse_group(enumerated):
            idx, g = enumerated
            if g is None:
                return 0
            if idx <= 2:  # hours, minutes, seconds
                return int(g)
            if idx == 3:  # nanoseconds
                g = g.ljust(9, "0")  # fill missing decimal places
                return int(g)
            else:  # utc offset
                if g == "Z":
                    return 0
                g = g.replace(":", "")
                g = g.ljust(5, "0")
                sign_, hours_, minutes_ = g[0], g[1:3], g[3:5]
                minutes_ = int(minutes_) + int(hours_) * 60
                return int(sign_.ljust(2, "1")) * minutes_ * 60

        super().__init__(value)
        hours, minutes, seconds, nanoseconds, utc_offset = map(
            parse_group, enumerate(self._groups)
        )
        self.nanoseconds = (nanoseconds
                            + seconds * 1000000000
                            + minutes * 60000000000
                            + hours * 3600000000000)
        self.utc_offset = utc_offset  # in seconds

    @classmethod
    def new(cls, nanoseconds: int, utc_offset_seconds: int):
        if nanoseconds < 0:
            raise ValueError("nanoseconds must be >= 0")
        hours, nanoseconds = divmod(nanoseconds, 3600000000000)
        minutes, nanoseconds = divmod(nanoseconds, 60000000000)
        seconds, nanoseconds = divmod(nanoseconds, 1000000000)
        offset_hours, offset_seconds = divmod(utc_offset_seconds, 3600)
        offset_minutes, offset_seconds = divmod(offset_seconds, 60)
        if offset_seconds:
            raise ValueError("UTC offset is expected in multiple of minutes")
        seconds_str = "%02i" % seconds
        if nanoseconds:
            seconds_str += "." + re.sub(r"0+$", "", "%09i" % nanoseconds)
        if utc_offset_seconds >= 0:
            s = "%02i:%02i:%s+%02i%02i" % (hours, minutes, seconds_str,
                                           offset_hours, offset_minutes)
        else:
            s = "%02i:%02i:%s-%02i%02i" % (
                hours, minutes, seconds_str,
                -offset_hours - 1, 60 - offset_minutes
            )
        return cls(s)

    def __eq__(self, other):
        if not isinstance(other, JoltTime):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("nanoseconds", "utc_offset"))

    def __repr__(self):
        return "%s<%r, %r>" % (self.__class__.__name__,
                               self.nanoseconds, self.utc_offset)


class JoltLocalTime(_JoltParsedType):
    _parse_re = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?")
    # yes:
    # 12:00:00.000000000
    # 12:00:00.000
    # 12:00:00
    # 12:00

    # no:
    # 12:00:00.0000000000
    # 12:0:0
    # 12

    def __init__(self, value: str):
        def parse_group(enumerated):
            idx, g = enumerated
            if g is None:
                return 0
            if idx <= 2:  # hours, minutes, seconds
                return int(g)
            else:  # nanoseconds
                g = g.ljust(9, "0")  # fill missing decimal places
                return int(g)

        super().__init__(value)
        hours, minutes, seconds, nanoseconds = map(
            parse_group, enumerate(self._groups)
        )
        self.nanoseconds = (nanoseconds
                            + seconds * 1000000000
                            + minutes * 60000000000
                            + hours * 3600000000000)

    @classmethod
    def new(cls, nanoseconds: int):
        if nanoseconds < 0:
            raise ValueError("nanoseconds must be >= 0")
        hours, nanoseconds = divmod(nanoseconds, 3600000000000)
        minutes, nanoseconds = divmod(nanoseconds, 60000000000)
        seconds, nanoseconds = divmod(nanoseconds, 1000000000)
        seconds_str = "%02i" % seconds
        if nanoseconds:
            seconds_str += "." + re.sub(r"0+$", "", "%09i" % nanoseconds)
        s = "%02i:%02i:%s" % (hours, minutes, seconds_str)
        return cls(s)

    def __eq__(self, other):
        if not isinstance(other, JoltLocalTime):
            return NotImplemented
        return self.nanoseconds == other.nanoseconds

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.nanoseconds)


class JoltDateTime(_JoltParsedType):
    _parse_re = re.compile(r"^("
                           + JoltDate._parse_re.pattern[1:-1]
                           + ")T("
                           + JoltTime._parse_re.pattern[1:-1]
                           + r")$")
    # yes:
    # <date_re>T<time_re>
    # where <date_re> is anything that works for JoltDate and <time_re> is
    # anything that works for JoltTime

    # no:
    # anything else
    def __init__(self, value: str):
        super().__init__(value)
        self.date = JoltDate(self._groups[0])
        self.time = JoltTime(self._groups[4])

    def __eq__(self, other):
        if not isinstance(other, JoltDateTime):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("date", "time"))

    def __repr__(self):
        return "%s<%r, %r>" % (self.__class__.__name__, self.date, self.time)

    @property
    def seconds_nanoseconds(self):
        s, ns = divmod(self.time.nanoseconds, 1000000000)
        # NOTE: it's called `simple_jolt`. Ignoring leap seconds and daylight
        #       saving time
        s += self.date.days * 86400
        return s, ns

    @classmethod
    def new(cls, seconds: int, nanoseconds: int, utc_offset_seconds: int):
        seconds += nanoseconds // 1000000000
        nanoseconds = nanoseconds % 1000000000
        days, seconds = divmod(seconds, 86400)
        date = JoltDate.new(days=days)
        time = JoltTime.new(nanoseconds=seconds * 1000000000 + nanoseconds,
                            utc_offset_seconds=utc_offset_seconds)
        return cls("%sT%s" % (date, time))


class JoltLocalDateTime(_JoltParsedType):
    _parse_re = re.compile(r"^("
                           + JoltDate._parse_re.pattern[1:-1]
                           + ")T("
                           + JoltLocalTime._parse_re.pattern[1:-1]
                           + r")$")
    # yes:
    # <date_re>T<local_time_re>
    # where <date_re> is anything that works for JoltDate and <local_time_re>
    # is anything that works for JoltLocalTime

    # no:
    # anything else
    def __init__(self, value: str):
        super().__init__(value)
        print(self._groups)
        self.date = JoltDate(self._groups[0])
        self.time = JoltLocalTime(self._groups[4])
    pass

    def __eq__(self, other):
        if not isinstance(other, JoltLocalDateTime):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("date", "time"))

    def __repr__(self):
        return "%s<%r, %r>" % (self.__class__.__name__, self.date, self.time)

    @property
    def seconds_nanoseconds(self):
        s, ns = divmod(self.time.nanoseconds, 1000000000)
        # NOTE: it's called `simple_jolt`. Ignoring leap seconds and daylight
        #       saving time
        s += self.date.days * 86400
        return s, ns

    @classmethod
    def new(cls, seconds: int, nanoseconds: int):
        seconds += nanoseconds // 1000000000
        nanoseconds = nanoseconds % 1000000000
        days, seconds = divmod(seconds, 86400)
        date = JoltDate.new(days=days)
        time = JoltLocalTime.new(
            nanoseconds=seconds * 1000000000 + nanoseconds
        )
        return cls("%sT%s" % (date, time))


class JoltDuration(_JoltParsedType):
    _parse_re = re.compile(
        r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)"
        r"(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)(?:\.(\d{1,9}))?S)?)?"
    )
    # yes:
    # P12Y13M40DT10H70M80.000000000S
    # P12Y
    # PT70M
    # P12T10H70M

    # no:
    # P12Y13M40DT10H70M80.0000000000S
    # P12Y13M40DT10H70.1M10S
    # P5W

    def __init__(self, value: str):
        def parse_group(enumerated):
            idx, g = enumerated
            if g is None:
                return 0
            if idx <= 5:  # years, months, days, hours, minutes, seconds
                return int(g)
            else:  # nanoseconds
                g = g.ljust(9, "0")  # fill missing decimal places
                return int(g)

        super().__init__(value)
        years, months, days, hours, minutes, seconds, nanoseconds = map(
            parse_group, enumerate(self._groups)
        )
        self.months = months + 12 * years
        self.days = days
        self.seconds = (seconds
                        + minutes * 60
                        + hours * 3600)
        self.nanoseconds = nanoseconds

    def __eq__(self, other):
        if not isinstance(other, JoltDuration):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("months", "days", "seconds"))

    def __repr__(self):
        return "%s<%r, %r, %r>" % (self.__class__.__name__, self.months,
                                   self.days, self.seconds)

    @classmethod
    def new(cls, months: int, days: int, seconds: int, nanoseconds: int):
        years, months = divmod(months, 12)
        seconds += nanoseconds // 1000000000
        nanoseconds = nanoseconds % 1000000000
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        years_str = "%02iY" % years if years else ""
        months_str = "%02iM" % months if months else ""
        days_str = "%02iD" % days if days else ""
        hours_str = "%02iH" % hours if hours else ""
        minutes_str = "%02iM" % minutes if minutes else ""
        seconds_str = "%02i" % seconds
        if nanoseconds:
            seconds_str += "." + re.sub(r"0+$", "", "%09i" % nanoseconds)
        seconds_str += "S"
        return cls("P%s%s%sT%s%s%s" % (years_str, months_str, days_str,
                                       hours_str, minutes_str, seconds_str))


class JoltPoint(_JoltParsedType):
    _parse_re = re.compile(
        r"^(?:SRID=(\d+);)?\s*"
        r"POINT\s*\(((?:[+-]?\d+(?:\.\d+)? ){1,2}[+-]?\d+(?:\.\d+)?)\)$"
    )
    # yes:
    # SRID=1234;POINT(1 2 3)
    # SRID=1234; POINT(1 2 3)
    # POINT(1 2 3)
    # POINT(1 2)
    # POINT(1 2.3)
    # POINT(1 -2.3)
    # POINT(+1 -2.3)

    # no:
    # POINT(1 2 3 4)
    # POINT(1)
    # POINT(1, 2)
    # POiNT(1 2)

    def __init__(self, value: str):
        super().__init__(value)
        srid, coords = self._groups
        coords = coords.split()
        self.srid = None if srid is None else int(srid)
        self.x = float(coords[0])
        self.y = float(coords[1])
        self.z = float(coords[2]) if len(coords) == 3 else None

    def __eq__(self, other):
        if not isinstance(other, JoltPoint):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("srid", "x", "y", "z"))

    def __repr__(self):
        return "%s<%r, %r, %r, %r>" % (self.__class__.__name__, self.srid,
                                       self.x, self.y, self.z)

    @classmethod
    def new(cls, x: float, y: float, z: float = None, srid: int = None):
        str_ = ""
        if srid is not None:
            str_ += "SRID=%i;" % srid
        str_ += "POINT("
        if z is not None:
            str_ += " ".join(map(str, (x, y, z)))
        else:
            str_ += " ".join(map(str, (x, y)))
        str_ += ")"
        return cls(str_)


class JoltNode(JoltType):
    def __init__(self, id_, labels, properties, element_id=None):
        self.id = id_
        self.labels = labels
        self.properties = properties
        self.element_id = element_id

    def __eq__(self, other):
        if not isinstance(other, JoltNode):
            return NotImplemented

        if self.element_id is None:
            return all(getattr(self, attr) == getattr(other, attr)
                       for attr in ("id", "labels", "properties"))
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "element_id", "labels", "properties"))

    def __repr__(self):
        if self.element_id is None:
            return "%s<%r, %r, %r>" % (self.__class__.__name__, self.id,
                                       self.labels, self.properties)
        return "%s<%r, %r, %r, %r>" % (self.__class__.__name__, self.id,
                                       self.labels, self.properties,
                                       self.element_id)


class JoltRelationship(JoltType):
    def __init__(self, id_, start_node_id, rel_type, end_node_id, properties):
        self.id = id_
        self.start_node_id = start_node_id
        self.rel_type = rel_type
        self.end_node_id = end_node_id
        self.properties = properties

    def __eq__(self, other):
        if not isinstance(other, JoltRelationship):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "start_node_id", "rel_type",
                                "end_node_id", "properties"))

    def __repr__(self):
        return "%s<%r, %r, %r, %r, %r>" % (
            self.__class__.__name__, self.id, self.start_node_id,
            self.rel_type, self.end_node_id, self.properties
        )


class JoltPath(JoltType):
    def __init__(self, *path):
        self.path = path

    def __eq__(self, other):
        if not isinstance(other, JoltPath):
            return NotImplemented
        return self.path == other.path

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.path)


class JoltWildcard(JoltType):
    """
    This is a stub-server specific JOLT type that marks a match-all object.

    e.g. `{"Z": "*"}` represents any integer.
    """

    def __init__(self, types):
        self.types = types


__all__ = [
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
]
