from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Null",)


class Null(HttpType):
    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Null", None).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        value_dict = ValueDict.deserialize(value)
        if value_dict is None:
            return None
        if value_dict.type_name != "Null":
            return None

        v = value_dict.value
        if v is not None:
            value_dict.invalid_value(f"must be None, was {type(v)}")
            return None

        return cls()

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if value is None:
            return cls()
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherNull):
            return cls()
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return True
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"
