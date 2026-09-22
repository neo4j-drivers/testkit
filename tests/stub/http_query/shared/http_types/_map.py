from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Map",)


class Map(HttpType):
    value: dict[str, HttpType]

    def __init__(self, value: t.Mapping[str, HttpType]) -> None:
        self.value = dict(value)

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        serialized_map: dict[str, t.Any] = {
            k: v.serialize(protocol_version) for k, v in self.value.items()
        }
        return ValueDict("Map", serialized_map).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        vd = ValueDict.deserialize(value)
        if vd is None:
            return None
        if vd.type_name != "Map":
            return None

        raw = vd.value
        if not isinstance(raw, dict):
            vd.invalid_value(f"must be dict, was {type(raw)}")
            return None

        elements: dict[str, HttpType] = {}
        for k, v in raw.items():
            if not isinstance(k, str):
                vd.invalid_value(
                    f"keys must be string, found {k!r} ({type(k)})"
                )
                return None
            http_item = HttpType.deserialize(v, protocol_version)
            if http_item is None:
                vd.invalid_value(f"invalid map item at key {k!r}")
                return None
            elements[k] = http_item

        return cls(elements)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if not isinstance(value, dict):
            return None

        elements: dict[str, HttpType] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                return None
            try:
                http_v = HttpType.from_native(v)
            except NotImplementedError:
                return None
            elements[k] = http_v
        return cls(elements)

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if not isinstance(value, types.CypherMap):
            return None

        elements: dict[str, HttpType] = {}
        # assume CypherMap.value is a mapping of str -> cypher-typed values
        for k, v in value.value.items():
            if not isinstance(k, str):
                return None
            try:
                http_v = HttpType.from_cypher_type(v)
            except NotImplementedError:
                return None
            elements[k] = http_v
        return cls(elements)

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.value == t.cast(Map, other).value
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.value!r}>"
