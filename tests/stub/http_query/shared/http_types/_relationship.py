from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Relationship",)


class Relationship(HttpType):
    element_id: str
    start_node_element_id: str
    end_node_element_id: str
    type_: str
    properties: dict[str, HttpType]

    def __init__(
        self,
        element_id: str,
        start_node_element_id: str,
        end_node_element_id: str,
        type_: str,
        properties: dict[str, HttpType],
    ) -> None:
        self.element_id = element_id
        self.start_node_element_id = start_node_element_id
        self.end_node_element_id = end_node_element_id
        self.type_ = type_
        self.properties = properties

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        value = {
            "_element_id": self.element_id,
            "_start_node_element_id": self.start_node_element_id,
            "_end_node_element_id": self.end_node_element_id,
            "_type": self.type_,
            "_properties": {
                k: v.serialize(protocol_version)
                for k, v in self.properties.items()
            },
        }
        return ValueDict("Relationship", value).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        # The server never accepts Relationships
        return None

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherRelationship):
            if (
                isinstance(value.elementId, types.CypherNull)
                or value.elementId is None
            ):
                raise ValueError("Relationship element_id cannot be None")
            if (
                isinstance(value.startNodeElementId, types.CypherNull)
                or value.startNodeElementId is None
            ):
                raise ValueError(
                    "Relationship start_node_element_id cannot be None"
                )
            if (
                isinstance(value.endNodeElementId, types.CypherNull)
                or value.endNodeElementId is None
            ):
                raise ValueError(
                    "Relationship end_node_element_id cannot be None"
                )

            element_id = value.elementId.value
            start_node_element_id = value.startNodeElementId.value
            end_node_element_id = value.endNodeElementId.value
            type_ = value.type.value

            properties = {
                k: HttpType.from_cypher_type(v)
                for k, v in value.props.value.items()
            }
            return cls(
                element_id=element_id,
                start_node_element_id=start_node_element_id,
                end_node_element_id=end_node_element_id,
                type_=type_,
                properties=properties,
            )
        return None

    def __eq__(self, other: object, /) -> bool:
        if type(self) is type(other):
            o = t.cast(Relationship, other)
            return (
                self.element_id == o.element_id
                and self.start_node_element_id == o.start_node_element_id
                and self.end_node_element_id == o.end_node_element_id
                and self.type_ == o.type_
                and self.properties == o.properties
            )
        return NotImplemented

    def __repr__(self) -> str:
        attrs = {
            "_element_id": self.element_id,
            "_start_node_element_id": self.start_node_element_id,
            "_end_node_element_id": self.end_node_element_id,
            "_type": self.type_,
            "_properties": {k: repr(v) for k, v in self.properties.items()},
        }
        str_repr = " ".join(f"{k}={v!r}" for k, v in attrs.items())
        return f"<{type(self).__name__} {str_repr}>"
