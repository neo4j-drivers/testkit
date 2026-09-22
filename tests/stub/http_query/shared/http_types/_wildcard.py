from __future__ import annotations

import typing as t

import typing_extensions as te

from ._base import HttpType
from ._protocol_version import ProtocolVersion

__all__ = ("Wildcard",)


class Wildcard(HttpType):
    """
    Value that accepts anything during serialization.

    Cannot be serialized itself.
    """

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        raise TypeError("Wildcard cannot be serialized")

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        return None

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        return None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, HttpType):
            return True
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"
