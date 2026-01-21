from __future__ import annotations

import re

from nutkit import protocol as types
from tests.shared import (
    MAX_INT64,
    MIN_INT64,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)


class TestGraph(HttpTestCase):
    # HTTP Query API currently does not support graph types' id
    # (only elementId).
    # Therefore, we expect the driver to polyfill a 0

    def test_node(self):
        with self.server() as server:
            for cypher_value in (
                types.CypherNode(
                    id=types.as_cypher_type(0),
                    labels=types.as_cypher_type([]),
                    props=types.as_cypher_type({}),
                    elementId=types.as_cypher_type("node0"),
                ),
                types.CypherNode(
                    id=types.as_cypher_type(0),
                    labels=types.as_cypher_type(
                        ["Person", "Admin", "Developer", "✨"]
                    ),
                    props=types.as_cypher_type(
                        {
                            "name": "Alice",
                            "living": {
                                "next door 🚪": "to somebody 🥸",
                                "somebody?": True,
                            },
                            "sizes": [MAX_INT64, MIN_INT64],
                            "floaty things": [
                                0.0,
                                -0.0,
                                1.0,
                                float("nan"),
                                float("inf"),
                                float("+inf"),
                            ],
                            "🖼️": bytes(range(0, 256, 7)),
                            "home": types.CypherPoint(
                                "cartesian", 1.2, 2.3, -3.4
                            ),
                            "life_goals": [None],
                        }
                    ),
                    elementId=types.as_cypher_type("🪪"),
                ),
            ):
                http_value = http_types.HttpType.from_cypher_type(cypher_value)
                with self.subTest(x=vars(cypher_value)):
                    self._receive_session_run(server, cypher_value, http_value)

    def test_relationship(self):
        with self.server() as server:
            for cypher_value in (
                types.CypherRelationship(
                    id=types.as_cypher_type(0),
                    startNodeId=types.as_cypher_type(0),
                    endNodeId=types.as_cypher_type(0),
                    type=types.as_cypher_type("KNOWS"),
                    props=types.as_cypher_type({}),
                    elementId=types.as_cypher_type("rel0"),
                    startNodeElementId=types.as_cypher_type("node0"),
                    endNodeElementId=types.as_cypher_type("node1"),
                ),
                types.CypherRelationship(
                    id=types.as_cypher_type(0),
                    startNodeId=types.as_cypher_type(0),
                    endNodeId=types.as_cypher_type(0),
                    type=types.as_cypher_type("🤺"),
                    props=types.as_cypher_type(
                        {
                            "since": types.CypherDateTime(
                                2026, 1, 29, 9, 24, 18, 123400
                            ),
                            "score": {"min": MIN_INT64, "max": MAX_INT64},
                            "blob": bytes(range(0, 10)),
                            "floaty things": [
                                0.0,
                                -0.0,
                                1.0,
                                float("nan"),
                                float("inf"),
                                float("+inf"),
                            ],
                            "✨?": "Lots of ✨!",
                        }
                    ),
                    elementId=types.as_cypher_type("🪪"),
                    startNodeElementId=types.as_cypher_type("🚰"),
                    endNodeElementId=types.as_cypher_type("🪠"),
                ),
            ):
                http_value = http_types.HttpType.from_cypher_type(cypher_value)
                with self.subTest(x=vars(cypher_value)):
                    self._receive_session_run(server, cypher_value, http_value)

    def test_path(self):
        with self.server() as server:
            for cypher_value in (
                self._build_cypher_path("(n1)"),
                self._build_cypher_path("(n1)-[r1]->(n2)"),
                self._build_cypher_path("(n1)<-[r1]-(n2)"),
                self._build_cypher_path("(n1)-[r1]->(n1)"),
                self._build_cypher_path("(n1)<-[r1]-(n1)"),
                self._build_cypher_path("(n1)-[r1]->(n2)<-[r2]-(n1)"),
                self._build_cypher_path("(n1)-[r1]->(n2)<-[r1]-(n1)"),
                types.CypherPath(
                    nodes=types.CypherList(
                        [
                            types.CypherNode(
                                id=types.as_cypher_type(0),
                                labels=types.as_cypher_type(["A"]),
                                props=types.as_cypher_type({}),
                                elementId=types.as_cypher_type("node0"),
                            ),
                            types.CypherNode(
                                id=types.as_cypher_type(0),
                                labels=types.as_cypher_type(
                                    ["Person", "Admin", "Developer", "✨"]
                                ),
                                props=types.as_cypher_type(
                                    {
                                        "name": "Alice",
                                        "living": {
                                            "next door 🚪": "to somebody 🥸",
                                            "somebody?": True,
                                        },
                                        "sizes": [MAX_INT64, MIN_INT64],
                                        "floaty things": [
                                            0.0,
                                            -0.0,
                                            1.0,
                                            float("nan"),
                                            float("inf"),
                                            float("+inf"),
                                        ],
                                        "🖼️": bytes(range(0, 256, 7)),
                                        "home": types.CypherPoint(
                                            "cartesian", 1.2, 2.3, -3.4
                                        ),
                                        "life_goals": [None],
                                    }
                                ),
                                elementId=types.as_cypher_type("node1"),
                            ),
                            types.CypherNode(
                                id=types.as_cypher_type(0),
                                labels=types.as_cypher_type(["A"]),
                                props=types.as_cypher_type({}),
                                elementId=types.as_cypher_type("node0"),
                            ),
                        ]
                    ),
                    relationships=types.CypherList(
                        [
                            types.CypherRelationship(
                                id=types.as_cypher_type(0),
                                startNodeId=types.as_cypher_type(0),
                                endNodeId=types.as_cypher_type(0),
                                type=types.as_cypher_type("RELATES_TO"),
                                props=types.as_cypher_type(
                                    {
                                        "since": types.CypherDateTime(
                                            2026, 1, 29, 9, 24, 18, 123400
                                        ),
                                        "score": {
                                            "min": MIN_INT64,
                                            "max": MAX_INT64,
                                        },
                                        "blob": bytes(range(0, 10)),
                                        "floaty things": [
                                            0.0,
                                            -0.0,
                                            1.0,
                                            float("nan"),
                                            float("inf"),
                                            float("+inf"),
                                        ],
                                        "✨?": "Lots of ✨!",
                                    }
                                ),
                                elementId=types.as_cypher_type("rel0"),
                                startNodeElementId=types.as_cypher_type(
                                    "node0"
                                ),
                                endNodeElementId=types.as_cypher_type("node1"),
                            ),
                            types.CypherRelationship(
                                id=types.as_cypher_type(0),
                                startNodeId=types.as_cypher_type(0),
                                endNodeId=types.as_cypher_type(0),
                                type=types.as_cypher_type("RELATES_TO"),
                                props=types.as_cypher_type({}),
                                elementId=types.as_cypher_type("rel1"),
                                startNodeElementId=types.as_cypher_type(
                                    "node0"
                                ),
                                endNodeElementId=types.as_cypher_type("node1"),
                            ),
                        ]
                    ),
                ),
            ):
                http_value = http_types.HttpType.from_cypher_type(cypher_value)
                with self.subTest(x=vars(cypher_value)):
                    self._receive_session_run(server, cypher_value, http_value)

    _NODE_RE = re.compile(r"\((?P<nodeElementId>[^)]+)\)")
    _REL_RE = re.compile(r"\[(?P<relElementId>[^\]]+)\]")
    _REL_FWD_RE = re.compile(rf"-{_REL_RE.pattern}->{_NODE_RE.pattern}")
    _REL_REV_RE = re.compile(rf"<-{_REL_RE.pattern}-{_NODE_RE.pattern}")

    @classmethod
    def _build_cypher_path(cls, pattern: str) -> types.CypherPath:
        nodes = []
        relationships = []
        match = cls._NODE_RE.match(pattern)
        if not match:
            raise ValueError(f"Invalid path pattern after: {pattern}")
        nodes.append(cls._node_from_match(match))
        pattern = pattern[match.end():]
        while pattern:
            match = cls._REL_FWD_RE.match(pattern)
            if match:
                next_node = cls._node_from_match(match)
                rel = cls._relationship_from_match(match, nodes[-1], next_node)
                relationships.append(rel)
                nodes.append(next_node)
                pattern = pattern[match.end():]
                continue
            match = cls._REL_REV_RE.match(pattern)
            if match:
                next_node = cls._node_from_match(match)
                rel = cls._relationship_from_match(match, next_node, nodes[-1])
                relationships.append(rel)
                nodes.append(next_node)
                pattern = pattern[match.end():]
                continue
            raise ValueError(f"Invalid path pattern after: {pattern}")
        return types.CypherPath(
            nodes=types.as_cypher_type(nodes),
            relationships=types.as_cypher_type(relationships),
        )

    @classmethod
    def _node_from_match(cls, match: re.Match) -> types.CypherNode:
        element_id = match.group("nodeElementId")
        return types.CypherNode(
            id=types.as_cypher_type(0),
            labels=types.as_cypher_type(["Node"]),
            props=types.as_cypher_type({}),
            elementId=types.as_cypher_type(element_id),
        )

    @classmethod
    def _relationship_from_match(
        cls,
        match: re.Match,
        start_node: types.CypherNode,
        end_node: types.CypherNode,
    ) -> types.CypherRelationship:
        element_id = match.group("relElementId")
        return types.CypherRelationship(
            id=types.as_cypher_type(0),
            startNodeId=types.as_cypher_type(0),
            endNodeId=types.as_cypher_type(0),
            type=types.as_cypher_type("REL"),
            props=types.as_cypher_type({}),
            elementId=types.as_cypher_type(element_id),
            startNodeElementId=start_node.elementId,
            endNodeElementId=end_node.elementId,
        )
