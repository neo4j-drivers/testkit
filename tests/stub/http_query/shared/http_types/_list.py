from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("List",)


class List(HttpType):
    value: list[HttpType]

    def __init__(self, value: t.Iterable[HttpType]) -> None:
        self.value = list(value)

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        serialized_list: list[t.Any] = [
            elem.serialize(protocol_version) for elem in self.value
        ]
        return ValueDict("List", serialized_list).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        vd = ValueDict.deserialize(value)
        if vd is None:
            return None
        if vd.type_name != "List":
            return None

        raw = vd.value
        if not isinstance(raw, list):
            vd.invalid_value(f"must be list, was {type(raw)}")
            return None

        elements: list[HttpType] = []
        for idx, item in enumerate(raw):
            http_item = HttpType.deserialize(item, protocol_version)
            if http_item is None:
                vd.invalid_value(f"invalid list item at index {idx}: {item!r}")
                return None
            elements.append(http_item)

        return cls(elements)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if not isinstance(value, (list, tuple)):
            return None

        elements: list[HttpType] = []
        for el in value:
            try:
                http_el = HttpType.from_native(el)
            except NotImplementedError:
                return None
            elements.append(http_el)
        return cls(elements)

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if not isinstance(value, types.CypherList):
            return None

        elements: list[HttpType] = []
        for el in value.value:
            try:
                http_el = HttpType.from_cypher_type(el)
            except NotImplementedError:
                return None
            elements.append(http_el)
        return cls(elements)

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.value == t.cast(List, other).value
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.value!r}]>"
