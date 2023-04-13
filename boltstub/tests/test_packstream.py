# Copyright (c) "Neo4j,"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import inspect

import pytest

from ..bolt_protocol import Structure
from ..simple_jolt.v1 import types as jolt_v1_types
from ..simple_jolt.v2 import types as jolt_v2_types


@pytest.mark.parametrize(("packstream_version", "fields", "res"), (
    # Node
    (
        1,
        [Structure(b"\x4E", 1, ["l"], {}, packstream_version=1)],
        [jolt_v1_types.JoltNode(1, ["l"], {})]
    ),
    (
        2,
        [Structure(b"\x4E", 1, ["l"], {}, "1", packstream_version=2)],
        [jolt_v2_types.JoltNode(1, ["l"], {}, "1")]
    ),
    # Relationship
    (
        1,
        [Structure(b"\x52", 1, 2, 3, "l", {}, packstream_version=1)],
        [jolt_v1_types.JoltRelationship(1, 2, "l", 3, {})]
    ),
    (
        2,
        [Structure(b"\x52", 1, 2, 3, "l", {}, "1", "2", "3",
                   packstream_version=2)],
        [jolt_v2_types.JoltRelationship(1, 2, "l", 3, {}, "1", "2", "3")]
    ),
    (
        2,
        [Structure(b"\x52", None, None, None, "l", {}, "1", "2", "3",
                   packstream_version=2)],
        [jolt_v2_types.JoltRelationship(None, None, "l", None, {},
                                        "1", "2", "3")]
    ),
    # Path
    (
        1,
        [Structure(b"\x50",
                   [Structure(b"\x4E", 1, ["l"], {}, packstream_version=1),
                    Structure(b"\x4E", 2, ["l2"], {}, packstream_version=1)],
                   [Structure(b"\x72", 3, "l3", {}, packstream_version=1)],
                   [1, 1],
                   packstream_version=1)],
        [jolt_v1_types.JoltPath(
            jolt_v1_types.JoltNode(1, ["l"], {}),
            jolt_v1_types.JoltRelationship(3, 1, "l3", 2, {}),
            jolt_v1_types.JoltNode(2, ["l2"], {})
        )]
    ),
    (
        2,
        [Structure(b"\x50",
                   [Structure(b"\x4E", 1, ["l"], {}, "1",
                              packstream_version=2),
                    Structure(b"\x4E", 2, ["l2"], {}, "2",
                              packstream_version=2)],
                   [Structure(b"\x72", 3, "l3", {}, "3",
                              packstream_version=2)],
                   [1, 1],
                   packstream_version=2)],
        [jolt_v2_types.JoltPath(
            jolt_v2_types.JoltNode(1, ["l"], {}, "1"),
            jolt_v2_types.JoltRelationship(3, 1, "l3", 2, {}, "3", "1", "2"),
            jolt_v2_types.JoltNode(2, ["l2"], {}, "2")
        )]
    ),
    (
        2,
        [Structure(b"\x50",
                   [Structure(b"\x4E", None, ["l"], {}, "1",
                              packstream_version=2),
                    Structure(b"\x4E", None, ["l2"], {}, "2",
                              packstream_version=2)],
                   [Structure(b"\x72", None, "l3", {}, "3",
                              packstream_version=2)],
                   [1, 1],
                   packstream_version=2)],
        [jolt_v2_types.JoltPath(
            jolt_v2_types.JoltNode(None, ["l"], {}, "1"),
            jolt_v2_types.JoltRelationship(None, None, "l3", None, {},
                                           "3", "1", "2"),
            jolt_v2_types.JoltNode(None, ["l2"], {}, "2")
        )]
    ),
    # Date
    (
        1,
        [Structure(b"\x44", 2, packstream_version=1)],
        [jolt_v1_types.JoltDate("1970-01-03")]
    ),
    (
        2,
        [Structure(b"\x44", 2, packstream_version=2)],
        [jolt_v2_types.JoltDate("1970-01-03")]
    ),
    (
        1,
        [Structure(b"\x44", 2, packstream_version=1), 123],
        [jolt_v1_types.JoltDate("1970-01-03"), 123]
    ),
    (
        2,
        [Structure(b"\x44", 2, packstream_version=2), 123],
        [jolt_v2_types.JoltDate("1970-01-03"), 123]
    ),
    (
        1,
        [[Structure(b"\x44", 2, packstream_version=1), "a"], 123],
        [[jolt_v1_types.JoltDate("1970-01-03"), "a"], 123]
    ),
    (
        2,
        [[Structure(b"\x44", 2, packstream_version=2), "a"], 123],
        [[jolt_v2_types.JoltDate("1970-01-03"), "a"], 123]
    ),
    (
        1,
        [{"a": Structure(b"\x44", 2, packstream_version=1), "b": 123}],
        [{"a": jolt_v1_types.JoltDate("1970-01-03"), "b": 123}]
    ),
    (
        2,
        [{"a": Structure(b"\x44", 2, packstream_version=2), "b": 123}],
        [{"a": jolt_v2_types.JoltDate("1970-01-03"), "b": 123}]
    ),
    # Time
    (
        1,
        [Structure(b"\x54", 1000000000, 0, packstream_version=1)],
        [jolt_v1_types.JoltTime("00:00:01Z")]
    ),
    (
        2,
        [Structure(b"\x54", 1000000000, 0, packstream_version=2)],
        [jolt_v2_types.JoltTime("00:00:01Z")]
    ),
    # LocalTime
    (
        1,
        [Structure(b"\x74", 1000000000, packstream_version=1)],
        [jolt_v1_types.JoltLocalTime("00:00:01")]
    ),
    (
        2,
        [Structure(b"\x74", 1000000000, packstream_version=2)],
        [jolt_v2_types.JoltLocalTime("00:00:01")]
    ),
    # DateTime
    (
        1,
        [Structure(b"\x46", 121, 123, -3600, packstream_version=1)],
        [jolt_v1_types.JoltDateTime("1970-01-01T00:02:01.000000123-01")]
    ),
    (
        2,
        [Structure(b"\x49", 121 + 3600, 123, -3600, packstream_version=2)],
        [jolt_v2_types.JoltDateTime("1970-01-01T00:02:01.000000123-01")]
    ),
    # DateTimeZoneId
    (
        1,
        [Structure(b"\x66", 121, 123,
                   "Europe/Paris", packstream_version=1)],
        [jolt_v1_types.JoltDateTime(
            "1970-01-01T00:02:01.000000123+01[Europe/Paris]"
        )]
    ),
    (
        2,
        [Structure(b"\x69", 121 - 3600, 123,
                   "Europe/Paris", packstream_version=2)],
        [jolt_v2_types.JoltDateTime(
            "1970-01-01T00:02:01.000000123+01[Europe/Paris]"
        )]
    ),
    # LocalDateTime
    (
        1,
        [Structure(b"\x64", 121, 1230, packstream_version=1)],
        [jolt_v1_types.JoltLocalDateTime("1970-01-01T00:02:01.00000123")]
    ),
    (
        2,
        [Structure(b"\x64", 121, 1230, packstream_version=2)],
        [jolt_v2_types.JoltLocalDateTime("1970-01-01T00:02:01.00000123")]
    ),
    # Duration
    (
        1,
        [Structure(b"\x45", 13, 15, 17, 1900, packstream_version=1)],
        [jolt_v1_types.JoltDuration("P1Y1M15DT17.00000190S")]
    ),
    (
        2,
        [Structure(b"\x45", 13, 15, 17, 1900, packstream_version=2)],
        [jolt_v2_types.JoltDuration("P1Y1M15DT17.00000190S")]
    ),
    # Point2D
    (
        2,
        [Structure(b"\x58", 123, 1.2, 3.4, packstream_version=2)],
        [jolt_v2_types.JoltPoint("SRID=123;POINT(1.2 3.4)")]
    ),
    # Point3D
    (
        1,
        [Structure(b"\x59", 123, 1.2, 3.4, 5.6, packstream_version=1)],
        [jolt_v1_types.JoltPoint("SRID=123;POINT(1.2 3.4 5.6)")]
    ),
    (
        2,
        [Structure(b"\x59", 123, 1.2, 3.4, 5.6, packstream_version=2)],
        [jolt_v2_types.JoltPoint("SRID=123;POINT(1.2 3.4 5.6)")]
    ),
))
def test_struct_to_jolt_type(packstream_version, fields, res):
    struct = Structure(b"\x00", *fields, packstream_version=packstream_version)
    if inspect.isclass(res) and issubclass(res, BaseException):
        with pytest.raises(res):
            struct.fields_to_jolt_types()
    else:
        jolt_types = struct.fields_to_jolt_types()
        assert res == jolt_types
        assert all(
            jolt_types[i].__class__ == res[i].__class__
            for i in range(len(res))
        )
