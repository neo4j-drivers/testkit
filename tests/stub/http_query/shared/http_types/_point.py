from __future__ import annotations

import math
import re
import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Point",)

FLOAT_RE = re.compile(
    r"[-+]?(?:\d*\.\d+|\d+\.?\d*)(?:[eE][-+]?\d+)?"
    r"|[-+]?(?i:nan)"
    r"|[-+]?(?i:inf(?:inity)?)"
)
POINT_RE = re.compile(
    r"^SRID=(?P<srid>\d+);"
    r"\s*POINT(?:\s+(?P<type>Z))?\s*\("
    rf"\s*(?P<x>{FLOAT_RE.pattern})"
    rf"\s+(?P<y>{FLOAT_RE.pattern})"
    rf"(?:\s+(?P<z>{FLOAT_RE.pattern}))?"
    rf"\)$"
)


class Point(HttpType):
    srid: int
    x: float
    y: float
    z: float | None

    def __init__(
        self,
        srid: int,
        x: float,
        y: float,
        z: float | None = None,
    ) -> None:
        self.srid = srid
        self.x = x
        self.y = y
        self.z = z

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Point", self._str()).serialize()

    def _str(self) -> str:
        if self.z is None:
            coords = map(self._float_str, (self.x, self.y))
            point_type = "POINT"
        else:
            coords = map(self._float_str, (self.x, self.y, self.z))
            point_type = "POINT Z"

        return f"SRID={self.srid};{point_type} ({' '.join(coords)})"

    @staticmethod
    def _float_str(v: float) -> str:
        s = str(v)
        s = s.replace("inf", "Infinity").replace("nan", "NaN")
        return s

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        vd = ValueDict.deserialize(value)
        if vd is None:
            return None
        if vd.type_name != "Point":
            return None

        v = vd.value
        if not isinstance(v, str):
            vd.invalid_value(f"must be string, was {type(v)}")
            return None
        match = POINT_RE.fullmatch(v)
        if match is None:
            vd.invalid_value(f"didn't match point regex: {v!r}")
            return None

        srid = int(match.group("srid"))
        x = float(match.group("x"))
        y = float(match.group("y"))
        z_str = match.group("z")
        z = float(z_str) if z_str is not None else None
        type_ = match.group("type")
        if type_ == "Z" and z is None:
            vd.invalid_value("missing z coordinate for POINT Z")
            return None
        if not type_ and z is not None:
            vd.invalid_value("unexpected z coordinate for POINT")
            return None

        return cls(srid=srid, x=x, y=y, z=z)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        # There is not matching native Python type for spatial points
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherPoint):
            system = value.system
            x = float(value.x)
            y = float(value.y)
            z = None if value.z is None else float(value.z)
            if system == "cartesian":
                srid = 7203 if z is None else 9157
            elif system == "wgs84":
                srid = 4326 if z is None else 4979
            else:
                raise ValueError(f"Unsupported spatial system: {system}")
            return cls(srid=srid, x=x, y=y, z=z)
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            o = t.cast(Point, other)
            return (
                self.srid == o.srid
                and self._coord_eq(self.x, o.x)
                and self._coord_eq(self.y, o.y)
                and self._coord_eq(self.z, o.z)
            )
        return NotImplemented

    @staticmethod
    def _coord_eq(a: float | None, b: float | None) -> bool:
        if a is not None and b is not None:
            if math.isnan(a) and math.isnan(b):
                return True
            return math.copysign(1, a) == math.copysign(1, b)
        return a == b

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._str()}>"
