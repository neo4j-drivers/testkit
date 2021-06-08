import inspect

import pytest

from ..bolt_protocol import Structure
from ..simple_jolt import types as jolt_types


@pytest.mark.parametrize(("fields", "res"), (
    # Node
    (
        [Structure(b"\x4E", 1, ["l"], {})],
        [jolt_types.JoltNode(1, ["l"], {})]
    ),
    # Relationship
    (
        [Structure(b"\x52", 1, 2, 3, "l", {})],
        [jolt_types.JoltRelationship(1, 2, "l", 3, {})]
    ),
    # Path
    (
        [Structure(b"\x50",
                   [Structure(b"\x4E", 1, ["l"], {}),
                    Structure(b"\x4E", 2, ["l2"], {})],
                   [Structure(b"\x72", 3, "l3", {})],
                   [1, 3, 2])],
        [jolt_types.JoltPath(
            jolt_types.JoltNode(1, ["l"], {}),
            jolt_types.JoltRelationship(3, 1, "l3", 2, {}),
            jolt_types.JoltNode(2, ["l2"], {})
        )]
    ),
    # Date
    (
        [Structure(b"\x44", 2)],
        [jolt_types.JoltDate("1970-01-03")]
    ),
    (
        [Structure(b"\x44", 2), 123],
        [jolt_types.JoltDate("1970-01-03"), 123]
    ),    (
        [[Structure(b"\x44", 2), "a"], 123],
        [[jolt_types.JoltDate("1970-01-03"), "a"], 123]
    ),
    (
        [{"a": Structure(b"\x44", 2), "b": 123}],
        [{"a": jolt_types.JoltDate("1970-01-03"), "b": 123}]
    ),
    # Time
    (
        [Structure(b"\x54", 1000000000, 0)],
        [jolt_types.JoltTime("00:00:01Z")]
    ),
    # LocalTime
    (
        [Structure(b"\x74", 1000000000)],
        [jolt_types.JoltLocalTime("00:00:01")]
    ),
    # DateTime
    (
        [Structure(b"\x46", 121, 123, -3600)],
        [jolt_types.JoltDateTime("1970-01-01T00:02:01.000000123-01")]
    ),
    # DateTimeZoneId (has no jolt type)
    (
        [Structure(b"\x66", 121, 123, "Europe/Paris")],
        TypeError
    ),
    # LocalDateTime
    (
        [Structure(b"\x64", 121, 1230)],
        [jolt_types.JoltLocalDateTime("1970-01-01T00:02:01.00000123")]
    ),
    # Duration
    (
        [Structure(b"\x45", 13, 15, 17, 1900)],
        [jolt_types.JoltDuration("P1Y1M15DT17.00000190S")]
    ),
    # Point2D
    (
        [Structure(b"\x58", 123, 1.2, 3.4)],
        [jolt_types.JoltPoint("SRID=123;POINT(1.2 3.4)")]
    ),
    # Point3D
    (
        [Structure(b"\x59", 123, 1.2, 3.4, 5.6)],
        [jolt_types.JoltPoint("SRID=123;POINT(1.2 3.4 5.6)")]
    ),

))
def test_struct_to_jolt_type(fields, res):
    struct = Structure(b"\x00", *fields)
    if inspect.isclass(res) and issubclass(res, BaseException):
        with pytest.raises(res):
            struct.fields_to_jolt_types()
    else:
        assert res == struct.fields_to_jolt_types()
