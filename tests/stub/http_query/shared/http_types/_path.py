from __future__ import annotations

import typing as t

import typing_extensions as te

from nutkit import protocol as types

from ._base import (
    HttpType,
    ValueDict,
)
from ._node import Node
from ._protocol_version import ProtocolVersion
from ._relationship import Relationship

__all__ = ("Path",)


class Path(HttpType):
    start_node: Node
    links: list[tuple[Relationship, Node]]

    def __init__(
        self,
        start_node: Node,
        links: list[tuple[Relationship, Node]],
    ) -> None:
        self.start_node = start_node
        self.links = links
        self._validate()

    def _validate(self) -> None:
        current_node = self.start_node
        for idx, (rel, next_node) in enumerate(self.links):
            start = rel.start_node_element_id
            end = rel.end_node_element_id
            if not (
                (
                    start == current_node.element_id
                    and end == next_node.element_id
                )
                or (
                    end == current_node.element_id
                    and start == next_node.element_id
                )
            ):
                raise ValueError(
                    f"Relationship at {idx} (start id: {start}, "
                    f"end id: {end}) does not connect "
                    f"the surrounding nodes ({current_node.element_id}, and "
                    f"{next_node.element_id})"
                )
            current_node = next_node

    def serialize(
        self,
        protocol_version: ProtocolVersion,
    ) -> t.Any:
        elements = (
            self.start_node,
            *(elem for link in self.links for elem in link),
        )
        value = [elem.serialize(protocol_version) for elem in elements]
        return ValueDict("Path", value).serialize()

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
        if isinstance(value, types.CypherPath):
            cypher_nodes = value.nodes.value
            cypher_rels = value.relationships.value
            if not cypher_nodes:
                raise ValueError("Path must have at least one node")
            if len(cypher_nodes) != len(cypher_rels) + 1:
                raise ValueError(
                    "Path must have exactly one more node than relationships"
                )
            nodes = []
            for cypher_node in cypher_nodes:
                node = HttpType.from_cypher_type(cypher_node)
                if not isinstance(node, Node):
                    raise TypeError(
                        f"Expected Node, got {type(node).__name__}"
                    )
                nodes.append(node)
            relationships = []
            for cypher_rel in cypher_rels:
                rel = HttpType.from_cypher_type(cypher_rel)
                if not isinstance(rel, Relationship):
                    raise TypeError(
                        f"Expected Relationship, got {type(rel).__name__}"
                    )
                relationships.append(rel)
            start_node = nodes[0]
            links = list(zip(relationships, nodes[1:]))
            return cls(
                start_node=start_node,
                links=links,
            )
        return None

    def __eq__(self, other: object, /) -> bool:
        if type(self) is type(other):
            o = t.cast(Path, other)
            return self.start_node == o.start_node and self.links == o.links
        return NotImplemented

    def __repr__(self) -> str:
        elements = (
            self.start_node,
            *(elem for link in self.links for elem in link),
        )
        path_repr = "-".join(map(repr, elements))
        return f"<{type(self).__name__} ({path_repr})>"
