from __future__ import annotations

from nutkit import protocol as types
from tests.stub.http_query.datatypes._test_case import HttpDataTypeTestCase
from tests.stub.http_query.shared import http_types


class TestSpatial(HttpDataTypeTestCase):
    def test_point(self):
        with self.server() as server:
            for system in ("cartesian", "wgs84"):
                for coordinates in (
                    (0.0, 0.0),
                    (0.0, 0.0, 0.0),
                    # note: the server (tested with 2025.12) does not accept
                    #       non-finite values for points, but for
                    #       future-proofing we want drivers to be able to
                    #       handle them anyway.
                    (float("-inf"), float("inf"), float("nan")),
                    (-0.0, -1.0, 1.0),
                    # max/min exponent
                    (2**1023, 2**-1022),
                    # max/min mantissa
                    (
                        9007199254740991.0,
                        -9007199254740991.0,
                        -(2 + 1 + 2e-51),
                    ),
                ):
                    cypher_value = types.CypherPoint(system, *coordinates)
                    with self.subTest(x=cypher_value):
                        self._echo_session_run(
                            server,
                            cypher_value,
                            http_types.HttpType.from_cypher_type(cypher_value),
                        )
