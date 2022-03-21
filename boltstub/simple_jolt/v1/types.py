import datetime
import re

from ..common.types import _JoltParsedType
from ..common.types import JoltType as _JoltTypeCommon
from ..common.types import JoltWildcard


class JoltType(_JoltTypeCommon):  # version specific type base class
    pass


class JoltV1DateMixin(_JoltParsedType):
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
        if not isinstance(other, JoltV1DateMixin):
            return NotImplemented
        return self.days == other.days

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.days)


class JoltDate(JoltV1DateMixin, JoltType):
    pass


class JoltV1TimeMixin(_JoltParsedType):
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
        if not isinstance(other, JoltV1TimeMixin):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("nanoseconds", "utc_offset"))

    def __repr__(self):
        return "%s<%r, %r>" % (self.__class__.__name__,
                               self.nanoseconds, self.utc_offset)


class JoltTime(JoltV1TimeMixin, JoltType):
    pass


class JoltV1LocalTimeMixin(_JoltParsedType):
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
        if not isinstance(other, JoltV1LocalTimeMixin):
            return NotImplemented
        return self.nanoseconds == other.nanoseconds

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.nanoseconds)


class JoltLocalTime(JoltV1LocalTimeMixin, JoltType):
    pass


class JoltV1DateTimeMixin(_JoltParsedType):
    _parse_re = re.compile(r"^("
                           + JoltV1DateMixin._parse_re.pattern[1:-1]
                           + ")T("
                           + JoltV1TimeMixin._parse_re.pattern[1:-1]
                           + r")$")
    # yes:
    # <date_re>T<time_re>
    # where <date_re> is anything that works for JoltV1DateMixin and <time_re>
    # is anything that works for JoltV1TimeMixin

    # no:
    # anything else
    def __init__(self, value: str):
        super().__init__(value)
        self.date = JoltV1DateMixin(self._groups[0])
        self.time = JoltV1TimeMixin(self._groups[4])

    def __eq__(self, other):
        if not isinstance(other, JoltV1DateTimeMixin):
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
        date = JoltV1DateMixin.new(days=days)
        time = JoltV1TimeMixin.new(
            nanoseconds=seconds * 1000000000 + nanoseconds,
            utc_offset_seconds=utc_offset_seconds
        )
        return cls("%sT%s" % (date, time))


class JoltDateTime(JoltV1DateTimeMixin, JoltType):
    pass


class JoltV1LocalDateTimeMixin(_JoltParsedType):
    _parse_re = re.compile(r"^("
                           + JoltV1DateMixin._parse_re.pattern[1:-1]
                           + ")T("
                           + JoltV1LocalTimeMixin._parse_re.pattern[1:-1]
                           + r")$")
    # yes:
    # <date_re>T<local_time_re>
    # where <date_re> is anything that works for JoltV1DateMixin and
    # <local_time_re> is anything that works for JoltV1LocalTimeMixin

    # no:
    # anything else
    def __init__(self, value: str):
        super().__init__(value)
        print(self._groups)
        self.date = JoltV1DateMixin(self._groups[0])
        self.time = JoltV1LocalTimeMixin(self._groups[4])
    pass

    def __eq__(self, other):
        if not isinstance(other, JoltV1LocalDateTimeMixin):
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
        date = JoltV1DateMixin.new(days=days)
        time = JoltV1LocalTimeMixin.new(
            nanoseconds=seconds * 1000000000 + nanoseconds
        )
        return cls("%sT%s" % (date, time))


class JoltLocalDateTime(JoltV1LocalDateTimeMixin, JoltType):
    pass


class JoltV1DurationMixin(_JoltParsedType):
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
        if not isinstance(other, JoltV1DurationMixin):
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


class JoltDuration(JoltV1DurationMixin, JoltType):
    pass


class JoltV1PointMixin(_JoltParsedType):
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
        if not isinstance(other, JoltV1PointMixin):
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


class JoltPoint(JoltV1PointMixin, JoltType):
    pass


class JoltV1NodeMixin(_JoltTypeCommon):
    def __init__(self, id_, labels, properties):
        self.id = id_
        self.labels = labels
        self.properties = properties

    def __eq__(self, other):
        if not isinstance(other, JoltV1NodeMixin):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "labels", "properties"))

    def __repr__(self):
        return "%s<%r, %r, %r>" % (self.__class__.__name__, self.id,
                                   self.labels, self.properties)


class JoltNode(JoltV1NodeMixin, JoltType):
    pass


class JoltV1RelationshipMixin(_JoltTypeCommon):
    def __init__(self, id_, start_node_id, rel_type, end_node_id, properties):
        self.id = id_
        self.start_node_id = start_node_id
        self.rel_type = rel_type
        self.end_node_id = end_node_id
        self.properties = properties

    def __eq__(self, other):
        if not isinstance(other, JoltV1RelationshipMixin):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "start_node_id", "rel_type",
                                "end_node_id", "properties"))

    def __repr__(self):
        return "%s<%r, %r, %r, %r, %r>" % (
            self.__class__.__name__, self.id, self.start_node_id,
            self.rel_type, self.end_node_id, self.properties
        )


class JoltRelationship(JoltV1RelationshipMixin, JoltType):
    pass


class JoltV1PathMixin(_JoltTypeCommon):
    def __init__(self, *path):
        self.path = path

    def __eq__(self, other):
        if not isinstance(other, JoltV1PathMixin):
            return NotImplemented
        return self.path == other.path

    def __repr__(self):
        return "%s<%r>" % (self.__class__.__name__, self.path)


class JoltPath(JoltV1PathMixin, JoltType):
    pass


__all__ = [
    JoltType,
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
]
