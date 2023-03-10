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


import json

from ....simple_jolt.v1.types import (
    JoltDate,
    JoltDateTime,
    JoltDuration,
    JoltLocalDateTime,
    JoltLocalTime,
    JoltNode,
    JoltPath,
    JoltPoint,
    JoltRelationship,
    JoltTime,
)

V1_LOADS = (
    # (in, out)
    # none
    ("null", None),

    # bool - simple
    ("true", True),
    ("false", False),

    # bool - full
    ('{"?": true}', True),
    ('{"?": false}', False),

    # int/float - simple
    ("1", 1),
    ("0", 0),
    ("-1", -1),
    ("-2147483648", -2147483648),
    ("-2147483649", -2147483649.),
    ("2147483647", 2147483647),
    ("2147483648", 2147483648.),
    ("999999999999999999999999999999", 999999999999999999999999999999.),

    # int - full
    ('{"Z": "1"}', 1),
    ('{"Z": "0"}', 0),
    ('{"Z": "-1"}', -1),
    ('{"Z": "-2147483648"}', -2147483648),
    ('{"Z": "-2147483649"}', -2147483649),
    ('{"Z": "2147483647"}', 2147483647),
    ('{"Z": "2147483648"}', 2147483648),
    ('{"Z": "1.23"}', 1),
    (
        '{"Z": "999999999999999999999999999999"}',
        999999999999999999999999999999
    ),

    # float - full
    ('{"R": "1"}', 1.0),
    ('{"R": "1.0"}', 1.0),
    ('{"R": "1.23456789"}', 1.23456789),
    ('{"R": "-1.23456789"}', -1.23456789),
    ('{"R": "0.0"}', 0.0),
    ('{"R": "-0.0"}', -0.0),
    ('{"R": "NaN"}', float("nan")),
    ('{"R": "+Infinity"}', float("inf")),
    ('{"R": "-Infinity"}', float("-inf")),

    # str - simple
    ('""', ""),
    ('"abc"', "abc"),
    ('"ab\\u1234"', "ab\u1234"),

    # str - full
    ('{"U": ""}', ""),
    ('{"U": "abc"}', "abc"),
    ('{"U": "ab\\u1234"}', "ab\u1234"),

    # bytes
    ('{"#": ""}', b""),
    ('{"#": "1234"}', b"\x12\x34"),
    ('{"#": "12FF"}', b"\x12\xff"),
    ('{"#": "12 34"}', b"\x12\x34"),
    ('{"#": "12 FF"}', b"\x12\xff"),
    ('{"#": [3, 4, 255]}', bytes([3, 4, 255])),

    # list - simple
    ("[]", []),
    ("[1]", [1]),
    ('["a"]', ["a"]),
    ('[1, "a"]', [1, "a"]),
    ('[1, ["a"]]', [1, ["a"]]),

    # list - full
    ('{"[]": []}', []),
    ('{"[]": [{"Z": "1"}]}', [1]),
    ('{"[]": [{"U": "a"}]}', ["a"]),
    ('{"[]": [{"Z": "1"}, {"U": "a"}]}', [1, "a"]),
    ('{"[]": [{"Z": "1"}, {"[]": [{"U": "a"}]}]}', [1, ["a"]]),

    # list - mixed
    ('{"[]": [1, {"[]": [{"U": "a"}]}]}', [1, ["a"]]),

    # dict - full
    ('{"{}": {}}', {}),
    ('{"{}": {"a": 1}}', {"a": 1}),
    ('{"{}": {"#": "no bytes"}}', {"#": "no bytes"}),

    # dict - simple
    # NOTE: this is not officially supported by the JOLT specs but allows for
    #       better backwards compatibility with existing stubscripts
    ("{}", {}),
    ('{"U": "a", "Z": "1"}', {"U": "a", "Z": "1"}),
    ('{"foo": "bar"}', {"foo": "bar"}),

    # date - full
    ('{"T": "2020-01-01"}', JoltDate("2020-01-01")),
    ('{"T": "2020-01"}', JoltDate("2020-01")),
    ('{"T": "2020"}', JoltDate("2020")),

    # time - full
    ('{"T": "12:00:00Z"}', JoltTime("12:00:00Z")),
    ('{"T": "12:00:00.001+0130"}', JoltTime("12:00:00.001+0130")),
    ('{"T": "12:00:00-12:00"}', JoltTime("12:00:00-12:00")),

    # local time - full
    ('{"T": "12:00:00.000000001"}', JoltLocalTime("12:00:00.000000001")),
    ('{"T": "12:00:00"}', JoltLocalTime("12:00:00")),
    ('{"T": "12:00"}', JoltLocalTime("12:00")),

    # date time - full
    (
        '{"T": "2020-01-02T12:13:01+01"}',
        JoltDateTime("2020-01-02T12:13:01+01")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+01"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+0100"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+01:00"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+01[Europe/Stockholm]"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01[Europe/Stockholm]")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+0100[Europe/Stockholm]"}',
        JoltDateTime("2020-01-02T12:13:01.1234+0100[Europe/Stockholm]")
    ),
    (
        '{"T": "2020-01-02T12:13:01.1234+01:00[Europe/Stockholm]"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01:00[Europe/Stockholm]")
    ),

    # local date time - full
    (
        '{"T": "2020-01-02T12:13:01.1234"}',
        JoltLocalDateTime("2020-01-02T12:13:01.1234")
    ),

    # duration - full
    ('{"T": "P1Y13M2DT18M"}', JoltDuration("P1Y13M2DT18M")),

    # point - full
    ('{"@": "POINT(1 2)"}', JoltPoint("POINT(1 2)")),
    ('{"@": "POINT (1 2)"}', JoltPoint("POINT (1 2)")),
    ('{"@": "POINT(1 2.3)"}', JoltPoint("POINT(1 2.3)")),
    ('{"@": "POINT(56.21 13.43)"}', JoltPoint("POINT(56.21 13.43)")),
    ('{"@": "POINT(56.21 -13.43 12)"}', JoltPoint("POINT(56.21 -13.43 12)")),
    ('{"@": "SRID=4326;POINT(1 2.3)"}', JoltPoint("SRID=4326;POINT(1 2.3)")),

    # node
    (
        '{"()": [123, ["l1", "l2"], {"a": 42}]}',
        JoltNode(123, ["l1", "l2"], {"a": 42})
    ),
    (
        '{"()": [123, ["l1", "l2"], {"U": 42}]}',
        JoltNode(123, ["l1", "l2"], {"U": 42})
    ),
    (
        '{"()": [123, ["l1", "l2"], {"U": {"Z": "2147483648"}}]}',
        JoltNode(123, ["l1", "l2"], {"U": 2147483648})
    ),

    # relationship
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": "value"}]}',
        JoltRelationship(42, 123, "REVERTS_TO", 321, {"prop": "value"})
    ),
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": {"U": "value"}}]}',
        JoltRelationship(42, 123, "REVERTS_TO", 321, {"prop": "value"})
    ),
    (
        '{"<-": [42, 123, "REVERTS_TO", 321, {"prop": {"U": "value"}}]}',
        JoltRelationship(42, 321, "REVERTS_TO", 123, {"prop": "value"})
    ),

    # path
    (
        '{"..": ['
        '{"()": [1, ["l"], {}]}, '
        '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
        '{"()": [3, ["l"], {}]}, '
        '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
        '{"()": [1, ["l"], {}]}'
        ']}',  # noqa: Q000
        JoltPath(
            JoltNode(1, ["l"], {}),
            JoltRelationship(2, 1, "RELATES_TO", 3, {}),
            JoltNode(3, ["l"], {}),
            JoltRelationship(4, 3, "RELATES_TO", 1, {}),
            JoltNode(1, ["l"], {}),
        )
    ),
)


def _get_v1_explicit_loads():
    res = []

    for in_, out in V1_LOADS:
        loaded_in = json.loads(in_)
        if (
            isinstance(loaded_in, dict)
            and set(loaded_in.keys()) in (
                {"Z"}, {"R"}, {"U"}, {"#"}, {"[]"}, {"{}"}, {"T"}, {"@"},
                {"()"}, {"->"}, {"<-"}, {".."},
            )
        ):
            key = next(iter(loaded_in.keys()))
            loaded_in[key + "v1"] = loaded_in.pop(key)
            res.append((json.dumps(loaded_in), out))

    return tuple(res)


V1_EXPLICIT_LOADS = _get_v1_explicit_loads()
