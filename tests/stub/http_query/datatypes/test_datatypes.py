from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.stub.http_query.datatypes._test_case import HttpDataTypeTestCase
from tests.stub.http_query.shared import http_types

MIN_INT64: t.Final[int] = -(2**63)
MAX_INT64: t.Final[int] = (2**63) - 1


class TestTypes(HttpDataTypeTestCase):
    def test_null(self):
        with self.server() as server:
            self._echo_session_run(
                server,
                types.CypherNull(),
                http_types.Null(),
            )

    def test_bool(self):
        with self.server() as server:
            for value in (True, False):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.CypherBool(value),
                        http_types.Bool(value),
                    )

    def test_int(self):
        with self.server() as server:
            for value in (MIN_INT64, -1337, -1, 0, 1, 1337, MAX_INT64):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.CypherInt(value),
                        http_types.Int(value),
                    )

    def test_float(self):
        with self.server() as server:
            values = [
                0,
                0.0,
                float("inf"),
                float("-inf"),
                float("nan"),
                1,
                -1,
                2**1023,
                2**-1022,
                9007199254740991,
                -9007199254740991,
                -(2 + 1 + 2e-51),
            ]

            for value in values:
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.CypherFloat(value),
                        http_types.Float(value),
                    )

    def test_str(self):
        with self.server() as server:
            for value in (
                "1",
                "-17∂ßå®",
                "String",
                "🐒💘🍌",
                "",
                "é",
                "e\u0301",  # 'e' + combining acute accent
                "Å",
                "A\u030a",  # 'A' + combining ring above
            ):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.CypherString(value),
                        http_types.Str(value),
                    )

    def test_bytes(self):
        with self.server() as server:
            for value in (
                bytes([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF]),
                bytes([]),
            ):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.CypherBytes(value),
                        http_types.Bytes(value),
                    )

    def test_list(self):
        with self.server() as server:
            for value in (
                [],
                [1, float("nan"), None, True, b"EHLO!", "world!"],
                [[[[[[[[[None]]]]]]]]],
            ):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.as_cypher_type(value),
                        http_types.HttpType.from_native(value),
                    )

    def test_map(self):
        with self.server() as server:
            for value in (
                {},
                {
                    "a": 1,
                    "b": float("nan"),
                    "é": None,
                    "e\u0301": True,
                    "👋\ufe0f": b"EHLO!",
                    "🌍": "world!",
                },
                {
                    "nested": {
                        "map": {
                            "with": {
                                "a": {
                                    "list": [1, 2, 3, {"deep": "value"}],
                                },
                            },
                        },
                    },
                },
            ):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        types.as_cypher_type(value),
                        http_types.HttpType.from_native(value),
                    )
