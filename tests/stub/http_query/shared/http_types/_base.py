from __future__ import annotations

import abc
import typing as t
from dataclasses import dataclass

import typing_extensions as te

if t.TYPE_CHECKING:
    from ._protocol_version import ProtocolVersion

__all__ = (
    "HttpType",
    "ValueDict",
)


class HttpType:
    _types: t.ClassVar[list[type[HttpType]]] = []

    def __init_subclass__(cls) -> None:
        HttpType._types.append(cls)

    @abc.abstractmethod
    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any: ...

    @classmethod
    @abc.abstractmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None: ...

    @staticmethod
    def deserialize(
        value: object,
        protocol_version: ProtocolVersion,
    ) -> HttpType | None:
        for typ in HttpType._types:
            http_type = typ._deserialize(value, protocol_version)
            if http_type is not None:
                return http_type
        return None

    @classmethod
    @abc.abstractmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None: ...

    @staticmethod
    def from_native(value: object) -> HttpType:
        if isinstance(value, HttpType):
            return value
        for typ in HttpType._types:
            http_type = typ._from_native(value)
            if http_type is not None:
                return http_type
        raise NotImplementedError(f"Unimplemented native type: {type(value)}")

    @classmethod
    @abc.abstractmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None: ...

    @staticmethod
    def from_cypher_type(value: t.Any) -> HttpType:
        for typ in HttpType._types:
            http_type = typ._from_cypher_type(value)
            if http_type is not None:
                return http_type
        # if isinstance(value, types.CypherNull):
        #     pass
        # if isinstance(value, types.CypherList):
        #     pass
        # if isinstance(value, types.CypherMap):
        #     pass
        # if isinstance(value, types.CypherInt):
        #     pass
        # if isinstance(value, types.CypherBool):
        #     pass
        # if isinstance(value, types.CypherFloat):
        #     pass
        # if isinstance(value, types.CypherString):
        #     pass
        # if isinstance(value, types.CypherBytes):
        #     pass
        # if isinstance(value, types.CypherPoint):
        #     pass
        # if isinstance(value, types.CypherDate):
        #     pass
        # if isinstance(value, types.CypherTime):
        #     pass
        # if isinstance(value, types.CypherDateTime):
        #     pass
        # if isinstance(value, types.CypherDuration):
        #     pass
        # if isinstance(value, types.CypherVector):
        #     pass
        # if isinstance(value, types.CypherUnsupportedType):
        #     pass
        raise NotImplementedError(f"Unimplemented cypher type: {type(value)}")


@dataclass
class ValueDict:
    type_name: str
    value: t.Any

    def serialize(self) -> dict[str, t.Any]:
        return {"$type": self.type_name, "_value": self.value}

    @staticmethod
    def deserialize(value: t.Any) -> ValueDict | None:
        if not isinstance(value, dict):
            return None
        if not set(value.keys()) == {"$type", "_value"}:
            return None
        return ValueDict(value["$type"], value["_value"])

    def invalid_value(self, msg: str) -> None:
        print(
            "==!!!== WARNING ==!!!==\n"
            ">>> encountered invalid '_value' in type "
            f"{self.type_name}: {msg}\n"
            "==!!!== ==!!!== ==!!!=="
        )
