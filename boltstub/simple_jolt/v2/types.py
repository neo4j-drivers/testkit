import datetime

import pytz

from ..common.types import (
    JoltType,
    JoltWildcard,
)
from ..v1.types import (
    JoltV1DateMixin,
    JoltV1DateTimeMixin,
    JoltV1DurationMixin,
    JoltV1LocalDateTimeMixin,
    JoltV1LocalTimeMixin,
    JoltV1PathMixin,
    JoltV1PointMixin,
    JoltV1TimeMixin,
)


class JoltType(JoltType):  # version specific type base class
    pass


class JoltV2DateTimeMixin(JoltV1DateTimeMixin):
    @property
    def seconds_nanoseconds(self):
        # since UTC unix epoch
        utc_epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)
        elapsed = self._to_dt() - utc_epoch
        s = elapsed.days * 86400 + elapsed.seconds
        ns = elapsed.microseconds * 1000 + self._ns_buffer
        return s, ns

    @classmethod
    def _format_s_ns_tz_info(cls, seconds: int, nanoseconds: int, tz_info):
        # zone_id UTC unix epoch
        dt = datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)

        microseconds, buffered_ns = divmod(nanoseconds, 1000)
        dt += datetime.timedelta(seconds=seconds, microseconds=microseconds)

        dt = dt.astimezone(tz_info)

        return cls._format_dt(dt, buffered_ns)


class JoltV2NodeMixin(JoltType):
    def __init__(self, id_, labels, properties, element_id):
        self.id = id_
        self.labels = labels
        self.properties = properties
        self.element_id = element_id

    def __eq__(self, other):
        if not isinstance(other, JoltV2NodeMixin):
            return NotImplemented

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "element_id", "labels", "properties"))

    def __repr__(self):
        return "%s<%r, %r, %r, %r>" % (self.__class__.__name__, self.id,
                                       self.labels, self.properties,
                                       self.element_id)


class JoltNode(JoltV2NodeMixin, JoltType):
    pass


class JoltV2RelationshipMixin(JoltType):
    def __init__(self, id_, start_node_id, rel_type, end_node_id, properties,
                 element_id, start_node_element_id, end_node_element_id):
        self.id = id_
        self.start_node_id = start_node_id
        self.rel_type = rel_type
        self.end_node_id = end_node_id
        self.properties = properties
        self.element_id = element_id
        self.start_node_element_id = start_node_element_id
        self.end_node_element_id = end_node_element_id

    def __eq__(self, other):
        if not isinstance(other, JoltV2RelationshipMixin):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "start_node_id", "rel_type",
                                "end_node_id", "properties", "element_id",
                                "start_node_element_id",
                                "end_node_element_id"))

    def __repr__(self):
        return "%s<%r, %r, %r, %r, %r, %r, %r, %r>" % (
            self.__class__.__name__, self.id, self.start_node_id,
            self.rel_type, self.end_node_id, self.properties,
            self.element_id, self.start_node_element_id,
            self.end_node_element_id
        )


class JoltRelationship(JoltV2RelationshipMixin, JoltType):
    pass


# Types unchanged from v1


class JoltDate(JoltV1DateMixin, JoltType):
    pass


class JoltTime(JoltV1TimeMixin, JoltType):
    pass


class JoltLocalTime(JoltV1LocalTimeMixin, JoltType):
    pass


class JoltDateTime(JoltV2DateTimeMixin, JoltType):
    pass


class JoltLocalDateTime(JoltV1LocalDateTimeMixin, JoltType):
    pass


class JoltDuration(JoltV1DurationMixin, JoltType):
    pass


class JoltPoint(JoltV1PointMixin, JoltType):
    pass


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
