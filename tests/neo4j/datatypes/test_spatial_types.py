import nutkit.protocol as types
from tests.neo4j.datatypes._base import _TestTypesBase


class TestDataTypes(_TestTypesBase):

    required_features = (types.Feature.API_TYPE_SPATIAL,)

    def test_should_echo_spatial_point(self):
        coords = [
            (1.1, -2),
            (0, 2, 3.5),
            (0, 0, 0),
            (1.1, -2),
            (0, 2, 3.5),
            (0, 0, 0),
        ]
        systems = ["cartesian", "wgs84"]

        self._create_driver_and_session()
        for coord in coords:
            for system in systems:
                self._verify_can_echo(types.CypherPoint(system, *coord))

    def test_point_components(self):
        for system, coords, names in (
            ("cartesian", (1.1, -2), ("x", "y")),
            ("cartesian", (1.1, -2.0, 123456.789), ("x", "y", "z")),
            ("wgs84", (1.1, -2.0), ("x", "y")),
            ("wgs84", (1.1, -2.0), ("longitude", "latitude")),
            ("wgs84", (1.1, -2.0, 123456.789), ("x", "y", "z")),
            (
                "wgs84", (1.1, -2.0, 123456.789),
                ("longitude", "latitude", "height")
            ),
        ):
            with self.subTest(system=system, coords=coords, names=names):
                self._create_driver_and_session()
                point = types.CypherPoint(system, *coords)
                values = self._read_query_values(
                    "CYPHER runtime=interpreted "
                    "WITH $point AS point "
                    f"RETURN [{', '.join(f'point.{n}' for n in names)}]",
                    params={"point": point},
                )
                self.assertEqual(
                    values,
                    [types.CypherList(list(map(types.CypherFloat, coords)))],
                )

    def test_nested_point(self):
        for points in (
            [
                types.CypherPoint("cartesian", 1.1, -2.456),
                types.CypherPoint("cartesian", -123456789.1, 2.45655),
            ],
            [
                types.CypherPoint("cartesian", 1.1, -2.456, 123456789.999),
                types.CypherPoint("cartesian", -1.1, 2.456, -123456789.999),
            ],
            [
                types.CypherPoint("wgs84", 1.1, -2.0),
                types.CypherPoint("wgs84", 78.23456, -89.45),
            ],
            [
                types.CypherPoint("wgs84", 1.1, -2.0, 123456.789),
                types.CypherPoint("wgs84", 78.23456, -89.45, -123456.789),
            ],
        ):
            with self.subTest(points=points):
                self._create_driver_and_session()
                data = types.CypherList(points)
                values = self._write_query_values(
                    "CREATE (a {x:$x}) RETURN a.x", params={"x": data},
                )
                self.assertEqual(values, [data])

    def test_cypher_created_point(self):
        for s, p in (
            ("point({x:3, y:4})", ("cartesian", 3, 4)),
            ("point({x:3, y:4, z:5})", ("cartesian", 3, 4, 5)),
            ("point({longitude:3, latitude:4})", ("wgs84", 3, 4)),
            ("point({longitude:3, latitude:4, height:5})", ("wgs84", 3, 4, 5)),
        ):
            with self.subTest(s=s):
                self._create_driver_and_session()
                values = self._read_query_values(f"RETURN {s}")
                self.assertEqual(values, [types.CypherPoint(*p)])
