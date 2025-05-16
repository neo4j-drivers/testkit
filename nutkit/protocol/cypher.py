"""Encoding of cypher types in the TestKit protocol.

Represents types that is part of the bolt/cypher protocol that needs to be sent
as parameters to queries (from frontend to backend) and data retrieved as a
result from running a query (from backend to frontend). The values in record
response has instances of these types.

All cypher types are sent from backend as:
    {
        name: <class name>
        data: {
            <all instance variables>
        }
    }
"""


import datetime
import math


class CypherNull:
    """Represents null/nil as sent/received to/from the database."""

    def __init__(self, value=None):
        self.value = None

    def __str__(self):
        return "<null>"

    def __repr__(self):
        return "<{}>".format(self.__class__.__name__)

    def __eq__(self, other):
        return isinstance(other, CypherNull)


class CypherList:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(list(map(str, self.value)))

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherList) and other.value == self.value


class CypherMap:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str({k: str(str(self.value[k])) for k in self.value})

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherMap) and other.value == self.value


class CypherInt:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherInt) and other.value == self.value


class CypherBool:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherBool) and other.value == self.value


class CypherFloat:
    """This float type compares nan == nan as true intentionally.

    This type is meant for capturing what values are sent over the wire rather
    than true float arithmetics.
    """

    def __init__(self, value):
        self.value = value
        if isinstance(value, float):
            if math.isinf(value):
                self.value = "+Infinity" if value > 0 else "-Infinity"
            if math.isnan(value):
                self.value = "NaN"

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherFloat) and other.value == self.value


class CypherString:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherString) and other.value == self.value


class CypherBytes:
    def __init__(self, value):
        self.value = value
        if isinstance(value, (bytes, bytearray)):
            # e.g. "ff 01"
            self.value = " ".join("{:02x}".format(byte) for byte in value)

    def __str__(self):
        return self.value

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherBytes) and other.value == self.value


class Node:
    def __init__(self, id, labels, props, elementId=None):
        # TODO: remove once all backends support new style relationships
        if elementId is None:
            import warnings
            warnings.warn(  # noqa: B028
                "Backend needs to support new style IDs for nodes"
            )
        self.id = id
        self.labels = labels
        self.props = props
        self.elementId = elementId

    def __str__(self):
        return "Node(id={}, labels={}, props={}, elementId={})".format(
            self.id, self.labels, self.props, self.elementId
        )

    def __repr__(self):
        return "<{}(id={}, labels={}, props={}, elementId={})>".format(
            self.__class__.__name__, self.id, self.labels, self.props,
            self.elementId
        )

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "labels", "props", "elementId"))


# More in line with other naming
CypherNode = Node


class Relationship:
    def __init__(self, id, startNodeId, endNodeId, type, props,
                 elementId=None, startNodeElementId=None,
                 endNodeElementId=None):
        # TODO: remove once all backends support new style relationships
        if None in (elementId, startNodeElementId, endNodeElementId):
            import warnings
            warnings.warn(  # noqa: B028
                "Backend needs to support new style IDs for relationships"
            )
        self.id = id
        self.startNodeId = startNodeId
        self.endNodeId = endNodeId
        self.type = type
        self.props = props
        self.elementId = elementId
        self.startNodeElementId = startNodeElementId
        self.endNodeElementId = endNodeElementId

    def __str__(self):
        return (
            "Relationship(id={}, startNodeId={}, endNodeId={}, type={}, "
            "props={}, elementId={}, startNodeElementId={}, "
            "endNodeElementId={})".format(self.id, self.startNodeId,
                                          self.endNodeId,
                                          self.type, self.props,
                                          self.elementId,
                                          self.startNodeElementId,
                                          self.endNodeElementId)
        )

    def __repr__(self):
        return (
            "<{}(id={}, startNodeId={}, endNodeId={}, type={}, "
            "props={}, elementId={}, startNodeElementId={}, "
            "endNodeElementId={})>".format(self.__class__.__name__,
                                           self.id,
                                           self.startNodeId, self.endNodeId,
                                           self.type,
                                           self.props,
                                           self.elementId,
                                           self.startNodeElementId,
                                           self.endNodeElementId)
        )

    def __eq__(self, other):
        if not isinstance(other, Relationship):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "startNodeId", "endNodeId", "type",
                                "props", "elementId", "startNodeElementId",
                                "endNodeElementId"))


# More in line with other naming
CypherRelationship = Relationship


class Path:
    def __init__(self, nodes, relationships):
        self.nodes = nodes
        self.relationships = relationships

    def __str__(self):
        return "Path(nodes={}, relationships={})".format(
            self.nodes, self.relationships
        )

    def __repr__(self):
        return "<{}(nodes={}, relationships={})>".format(
            self.__class__.__name__, self.nodes, self.relationships
        )

    def __eq__(self, other):
        if not isinstance(other, Path):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("nodes", "relationships"))


# More in line with other naming
CypherPath = Path


class CypherPoint:
    def __init__(self, system, x, y, z=None):
        self.system = system
        self.x = x
        self.y = y
        self.z = z
        if system not in ("cartesian", "wgs84"):
            raise ValueError("Invalid system: {}".format(system))

    def __str__(self):
        if self.z is None:
            return "CypherPoint(system={}, x={}, y={})".format(
                self.system, self.x, self.y
            )
        return "CypherPoint(system={}, x={}, y={}, z={})".format(
            self.system, self.x, self.y, self.z
        )

    def __repr__(self):
        return "<{}(system={}, x={}, y={}, z={})>".format(
            self.__class__.__name__, self.system, self.x, self.y, self.z
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("system", "x", "y", "z"))


class CypherDate:
    def __init__(self, year, month, day):
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)
        for v in ("year", "month", "day"):
            if getattr(self, v) != locals()[v]:
                raise ValueError("{} must be integer".format(v))

    def __str__(self):
        return "CypherDate(year={}, month={}, day={})".format(
            self.year, self.month, self.day
        )

    def __repr__(self):
        return "<{}(year={}, month={}, day={})>".format(
            self.__class__.__name__, self.year, self.month, self.day
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("year", "month", "day"))


class CypherTime:
    def __init__(self, hour, minute, second, nanosecond, utc_offset_s=None):
        self.hour = int(hour)
        self.minute = int(minute)
        self.second = int(second)
        self.nanosecond = int(nanosecond)
        # seconds east of UTC or None for local time
        self.utc_offset_s = utc_offset_s
        if self.utc_offset_s is not None:
            self.utc_offset_s = int(utc_offset_s)
        for v in ("hour", "minute", "second", "nanosecond", "utc_offset_s"):
            if getattr(self, v) != locals()[v]:
                raise ValueError("{} must be integer".format(v))

    def __str__(self):
        return (
            "CypherTime(hour={}, minute={}, second={}, nanosecond={}, "
            "utc_offset_s={})".format(
                self.hour, self.minute, self.second, self.nanosecond,
                self.utc_offset_s
            )
        )

    def __repr__(self):
        return (
            "<{}(hour={}, minute={}, second={}, nanosecond={}, "
            "utc_offset_s={})>".format(
                self.__class__.__name__, self.hour, self.minute, self.second,
                self.nanosecond, self.utc_offset_s
            )
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("hour", "minute", "second", "nanosecond",
                                "utc_offset_s"))


class CypherDateTime:
    def __init__(self, year, month, day, hour, minute, second, nanosecond,
                 utc_offset_s=None, timezone_id=None):
        # The date time is always wall clock time (with or without timezone)
        # If timezone_id is given (e.g., "Europe/Stockholm"), utc_offset_s
        # must also be provided to avoid ambiguity.
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)
        self.hour = int(hour)
        self.minute = int(minute)
        self.second = int(second)
        self.nanosecond = int(nanosecond)

        self.utc_offset_s = utc_offset_s
        if self.utc_offset_s is not None:
            self.utc_offset_s = int(utc_offset_s)
        self.timezone_id = timezone_id
        if self.timezone_id is not None:
            self.timezone_id = str(timezone_id)

        for v in ("year", "month", "day", "hour", "minute", "second",
                  "nanosecond", "utc_offset_s"):
            if getattr(self, v) != locals()[v]:
                raise ValueError("{} must be integer".format(v))
        if timezone_id is not None and utc_offset_s is None:
            raise ValueError("utc_offset_s must be provided if timezone_id "
                             "is given")

    def __str__(self):
        return (
            "CypherDateTime(year={}, month={}, day={}, hour={}, minute={}, "
            "second={}, nanosecond={}, utc_offset_s={}, timezone_id={})"
            .format(
                self.year, self.month, self.day, self.hour, self.minute,
                self.second, self.nanosecond, self.utc_offset_s,
                self.timezone_id
            )
        )

    def __repr__(self):
        return (
            "<{}(year={}, month={}, day={}, hour={}, minute={}, second={}, "
            "nanosecond={}, utc_offset_s={}, timezone_id={})>"
            .format(
                self.__class__.__name__, self.year, self.month, self.day,
                self.hour, self.minute, self.second, self.nanosecond,
                self.utc_offset_s, self.timezone_id
            )
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("year", "month", "day", "hour", "minute",
                                "second", "nanosecond", "utc_offset_s",
                                "timezone_id"))

    def as_utc(self):
        if self.utc_offset_s is None:
            return self
        us, ns = divmod(self.nanosecond, 1000)
        dt = datetime.datetime(
            year=self.year, month=self.month, day=self.day, hour=self.hour,
            minute=self.minute, second=self.second, microsecond=us
        )
        utc_dt = dt - datetime.timedelta(seconds=self.utc_offset_s)

        return CypherDateTime(
            utc_dt.year, utc_dt.month, utc_dt.day, utc_dt.hour, utc_dt.minute,
            utc_dt.second, utc_dt.microsecond * 1000 + ns,
            utc_offset_s=0, timezone_id="UTC"
        )


class CypherDuration:
    def __init__(self, months, days, seconds, nanoseconds):
        self.months = int(months)
        self.days = int(days)
        seconds, nanoseconds = divmod(
            seconds * 1000000000 + nanoseconds, 1000000000
        )
        self.seconds = int(seconds)
        self.nanoseconds = int(nanoseconds)

        for v in ("months", "days", "seconds", "nanoseconds"):
            if getattr(self, v) != locals()[v]:
                raise ValueError("{} must be integer".format(v))

    def __str__(self):
        return (
            "CypherDuration(months={}, days={}, seconds={}, nanoseconds={})"
            .format(self.months, self.days, self.seconds, self.nanoseconds)
        )

    def __repr__(self):
        return (
            "<{}(months={}, days={}, seconds={}, nanoseconds={})>"
            .format(self.__class__.__name__, self.months, self.days,
                    self.seconds, self.nanoseconds)
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("months", "days", "seconds", "nanoseconds"))


def as_cypher_type(value):
    if value is None:
        return CypherNull()
    if isinstance(value, (list, tuple)):
        return CypherList([as_cypher_type(v) for v in value])
    if isinstance(value, dict):
        return CypherMap({k: as_cypher_type(v) for k, v in value.items()})
    if isinstance(value, bool):
        return CypherBool(value)
    if isinstance(value, int):
        return CypherInt(value)
    if isinstance(value, float):
        return CypherFloat(value)
    if isinstance(value, str):
        return CypherString(value)
    if isinstance(value, (bytes, bytearray)):
        return CypherBytes(value)
    if isinstance(
        value,
        (
            CypherNode,
            CypherRelationship,
            CypherPath,
            CypherPoint,
            CypherDate,
            CypherTime,
            CypherDateTime,
            CypherDuration,
        )
    ):
        return value
    raise TypeError("Unsupported type: {}".format(type(value)))
