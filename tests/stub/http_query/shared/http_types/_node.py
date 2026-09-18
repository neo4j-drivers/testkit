from __future__ import annotations

import typing as t
from collections import Counter

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._protocol_version import ProtocolVersion

__all__ = ("Node",)


class Node(HttpType):
    element_id: str
    labels: list[str]
    properties: dict[str, HttpType]

    def __init__(
        self,
        element_id: str,
        labels: list[str],
        properties: dict[str, HttpType],
    ) -> None:
        self.element_id = element_id
        self.labels = labels
        self.properties = properties

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        value = {
            "_element_id": self.element_id,
            "_labels": self.labels,
            "_properties": {
                k: v.serialize(protocol_version)
                for k, v in self.properties.items()
            },
        }
        return ValueDict("Node", value).serialize()

    @classmethod
    def _deserialize(
        cls,
        value: object,
        protocol_version: ProtocolVersion,
    ) -> te.Self | None:
        # The server never accepts Nodes
        return None

    @classmethod
    def _from_native(
        cls,
        value: object,
    ) -> te.Self | None:
        return None

    @classmethod
    def _from_cypher_type(cls, value: object) -> te.Self | None:
        if isinstance(value, types.CypherNode):
            if (
                isinstance(value.elementId, types.CypherNull)
                or value.elementId is None
            ):
                raise ValueError("Node element_id cannot be None")
            element_id = value.elementId.value
            labels = [label.value for label in value.labels.value]
            properties = {
                k: HttpType.from_cypher_type(v)
                for k, v in value.props.value.items()
            }
            return cls(
                element_id=element_id,
                labels=labels,
                properties=properties,
            )
        return None

    def __eq__(self, other: object, /) -> bool:
        if type(self) is type(other):
            o = t.cast(Node, other)
            return (
                self.element_id == o.element_id
                and Counter(self.labels) == Counter(o.labels)
                and self.properties == o.properties
            )
        return NotImplemented

    def __repr__(self) -> str:
        attrs = {
            "_element_id": self.element_id,
            "_labels": self.labels,
            "_properties": {k: repr(v) for k, v in self.properties.items()},
        }
        str_repr = " ".join(f"{k}={v!r}" for k, v in attrs.items())
        return f"<{type(self).__name__} {str_repr}>"
