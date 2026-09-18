from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Int",)


class Int(HttpType):
    value: int

    def __init__(self, value: int) -> None:
        self.value = value

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Integer", str(self.value)).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Integer":
            return None

        v = value_dict.value
        if not isinstance(v, str):
            value_dict.invalid_value(f"must be string, was {type(v)}")
            return None
        try:
            int_value = int(v)
        except ValueError as e:
            value_dict.invalid_value(f"invalid int string {v!r}: {e!r}")
            return None

        return cls(int_value)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if isinstance(value, int) and not isinstance(value, bool):
            return cls(value)
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherInt):
            return cls(value.value)
        return None

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.value!r}>"

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.value == t.cast(Int, other).value
        return NotImplemented
