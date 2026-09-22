from __future__ import annotations

import base64
import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Bytes",)


class Bytes(HttpType):
    value: bytes

    def __init__(self, value: t.Union[bytes, bytearray]) -> None:
        self.value = bytes(value)

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        return ValueDict("Base64", self._b64()).serialize()

    def _b64(self) -> str:
        return base64.b64encode(self.value).decode("ascii")

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        vd = ValueDict.deserialize(value)
        if vd is None:
            return None
        if vd.type_name != "Base64":
            return None

        v = vd.value
        if not isinstance(v, str):
            vd.invalid_value(f"must be string, was {type(v)}")
            return None
        try:
            raw = base64.b64decode(v, validate=True)
        except ValueError as e:
            vd.invalid_value(f"invalid base64 string: {e!r}")
            return None

        return cls(raw)

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        if isinstance(value, (bytes, bytearray)):
            return cls(value)
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherBytes):
            v = value.value
            if isinstance(v, str):
                try:
                    raw = bytes([int(byte, 16) for byte in v.split()])
                except ValueError:
                    return None
                return cls(raw)
        return None

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.value == t.cast(Bytes, other).value
        return NotImplemented

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._b64()!r}>"
