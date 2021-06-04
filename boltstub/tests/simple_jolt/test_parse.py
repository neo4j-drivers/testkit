import inspect
import math

import pytest

from ...simple_jolt import (
    dumps_full,
    dumps_simple,
    loads,
)
from ...simple_jolt.types import (
    JoltDate,
    JoltTime,
    JoltLocalTime,
    JoltDateTime,
    JoltLocalDateTime,
    JoltDuration,
    JoltPoint,
    JoltNode,
    JoltRelationship,
    JoltPath,
)


@pytest.mark.parametrize(("in_", "out_"), (
    # none
    (None, "null"),

    # bool
    (True, '{"?": true}'),
    (False, '{"?": false}'),

    # int
    (1, '{"Z": "1"}'),
    (0, '{"Z": "0"}'),
    (-1, '{"Z": "-1"}'),
    (-2147483649, '{"Z": "-2147483649"}'),
    (2147483648, '{"Z": "2147483648"}'),
    (999999999999999999999999999999, '{"Z": "999999999999999999999999999999"}'),

    # float
    (1.0, '{"R": "1.0"}'),
    (1.23456789, '{"R": "1.23456789"}'),
    (-1.23456789, '{"R": "-1.23456789"}'),
    (0.0, '{"R": "0.0"}'),
    (-0.0, '{"R": "-0.0"}'),
    (float("nan"), '{"R": "NaN"}'),
    (float("inf"), '{"R": "+Infinity"}'),
    (float("-inf"), '{"R": "-Infinity"}'),

    # str
    ("abc", '{"U": "abc"}'),
    ("ab\u1234", '{"U": "ab\\u1234"}'),

    # bytes
    (b"\x12\x34", ('{"#": "1234"}', '{"#": "12 34"}')),
    (b"\x12\xff", ('{"#": "12FF"}', '{"#": "12 FF"}')),
    (bytearray((0x12, 0x34)), ('{"#": "1234"}', '{"#": "12 34"}')),
    (bytearray((0x12, 0xff)), ('{"#": "12FF"}', '{"#": "12 FF"}')),

    # list
    ([], '{"[]": []}'),
    ([1], '{"[]": [{"Z": "1"}]}'),
    (["a"], '{"[]": [{"U": "a"}]}'),
    ([1, "a"], '{"[]": [{"Z": "1"}, {"U": "a"}]}'),
    ([1, ["a"]], '{"[]": [{"Z": "1"}, {"[]": [{"U": "a"}]}]}'),

    # tuple
    (tuple(), '{"[]": []}'),
    ((1,), '{"[]": [{"Z": "1"}]}'),
    (("a",), '{"[]": [{"U": "a"}]}'),
    ((1, "a"), '{"[]": [{"Z": "1"}, {"U": "a"}]}'),
    ((1, ["a"]), '{"[]": [{"Z": "1"}, {"[]": [{"U": "a"}]}]}'),

    # dict
    ({}, '{"{}": {}}'),
    ({"a": 1}, '{"{}": {"a": {"Z": "1"}}}'),
    ({"#": "no bytes"}, '{"{}": {"#": {"U": "no bytes"}}}'),
    ({1: 1}, ValueError),

    # date
    (JoltDate("2020-01-01"), '{"T": "2020-01-01"}'),
    (JoltDate("2020-01"), '{"T": "2020-01"}'),
    (JoltDate("2020"), '{"T": "2020"}'),

    # time
    (JoltTime("12:00:00Z"), '{"T": "12:00:00Z"}'),
    (JoltTime("12:00:00.001+0130"), '{"T": "12:00:00.001+0130"}'),
    (JoltTime("12:00:00-12:00"), '{"T": "12:00:00-12:00"}'),

    # local time
    (JoltLocalTime("12:00:00.000000001"), '{"T": "12:00:00.000000001"}'),
    (JoltLocalTime("12:00:00"), '{"T": "12:00:00"}'),
    (JoltLocalTime("12:00"), '{"T": "12:00"}'),

    # date time
    (
        JoltDateTime("2020-01-02T12:13:01.1234+01"),
        '{"T": "2020-01-02T12:13:01.1234+01"}'
    ),

    # local date time
    (
        JoltLocalDateTime("2020-01-02T12:13:01.1234"),
        '{"T": "2020-01-02T12:13:01.1234"}'
    ),

    # duration
    (JoltDuration("P1Y13M2DT18M"), '{"T": "P1Y13M2DT18M"}'),

    # point
    (JoltPoint("POINT(1 2)"), '{"@": "POINT(1 2)"}'),
    (JoltPoint("POINT (1 2)"), '{"@": "POINT (1 2)"}'),
    (JoltPoint("POINT(1 2.3)"), '{"@": "POINT(1 2.3)"}'),
    (JoltPoint("POINT(56.21 13.43)"), '{"@": "POINT(56.21 13.43)"}'),
    (JoltPoint("POINT(56.21 13.43 12)"), '{"@": "POINT(56.21 13.43 12)"}'),
    (JoltPoint("SRID=4326;POINT(1 2.3)"), '{"@": "SRID=4326;POINT(1 2.3)"}'),

    # node
    (
        JoltNode(123, ["l1", "l2"], {"a": 42}),
        '{"()": [123, ["l1", "l2"], {"a": {"Z": "42"}}]}'
    ),
    (
        JoltNode(123, ["l1", "l2"], {"U": 42}),
        '{"()": [123, ["l1", "l2"], {"U": {"Z": "42"}}]}'
    ),

    # relationship
    (
        JoltRelationship(42, 123, "REVERTS_TO", 321, {"prop": "value"}),
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": {"U": "value"}}]}'
    ),

    # path
    (
        JoltPath(
            JoltNode(1, ["l"], {}),
            JoltRelationship(2, 1, "RELATES_TO", 3, {}),
            JoltNode(3, ["l"], {}),
            JoltRelationship(4, 3, "RELATES_TO", 1, {}),
            JoltNode(1, ["l"], {}),
        ),
        '{"..": ['
        '{"()": [1, ["l"], {}]}, '
        '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
        '{"()": [3, ["l"], {}]}, '
        '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
        '{"()": [1, ["l"], {}]}'
        ']}'
    ),
))
@pytest.mark.parametrize("human_readable", [True, False])
def test_dumps_full(in_, out_, human_readable):
    if isinstance(out_, tuple):
        out_ = out_[human_readable]
    if inspect.isclass(out_) and issubclass(out_, BaseException):
        with pytest.raises(out_):
            dumps_full(in_)
    else:
        assert dumps_full(in_, human_readable=human_readable) == out_


@pytest.mark.parametrize(("in_", "out_"), (
    # none
    (None, "null"),

    # bool
    (True, "true"),
    (False, "false"),

    # int
    (1, "1"),
    (0, "0"),
    (-1, "-1"),
    (-2147483648, "-2147483648"),
    (-2147483649, '{"Z": "-2147483649"}'),
    (2147483647, "2147483647"),
    (2147483648, '{"Z": "2147483648"}'),
    (999999999999999999999999999999, '{"Z": "999999999999999999999999999999"}'),

    # float
    (1.0, '{"R": "1.0"}'),
    (1.23456789, '{"R": "1.23456789"}'),
    (-1.23456789, '{"R": "-1.23456789"}'),
    (0.0, '{"R": "0.0"}'),
    (-0.0, '{"R": "-0.0"}'),
    (float("nan"), '{"R": "NaN"}'),
    (float("inf"), '{"R": "+Infinity"}'),
    (float("-inf"), '{"R": "-Infinity"}'),

    # str
    ("abc", '"abc"'),
    ("ab\u1234", '"ab\\u1234"'),

    # bytes
    (b"\x12\x34", ('{"#": "1234"}', '{"#": "12 34"}')),
    (b"\x12\xff", ('{"#": "12FF"}', '{"#": "12 FF"}')),
    (bytearray((0x12, 0x34)), ('{"#": "1234"}', '{"#": "12 34"}')),
    (bytearray((0x12, 0xff)), ('{"#": "12FF"}', '{"#": "12 FF"}')),

    # list
    ([], "[]"),
    ([1], "[1]"),
    (["a"], '["a"]'),
    ([1, "a"], '[1, "a"]'),
    ([1, ["a"]], '[1, ["a"]]'),

    # tuple
    (tuple(), "[]"),
    ((1,), '[1]'),
    (("a",), '["a"]'),
    ((1, "a"), '[1, "a"]'),
    ((1, ["a"]), '[1, ["a"]]'),

    # dict
    ({}, '{"{}": {}}'),
    ({"a": 1}, '{"{}": {"a": 1}}'),
    ({"#": "no bytes"}, '{"{}": {"#": "no bytes"}}'),
    ({1: 1}, ValueError),

    # date
    (JoltDate("2020-01-01"), '{"T": "2020-01-01"}'),
    (JoltDate("2020-01"), '{"T": "2020-01"}'),
    (JoltDate("2020"), '{"T": "2020"}'),

    # time
    (JoltTime("12:00:00Z"), '{"T": "12:00:00Z"}'),
    (JoltTime("12:00:00.001+0130"), '{"T": "12:00:00.001+0130"}'),
    (JoltTime("12:00:00-12:00"), '{"T": "12:00:00-12:00"}'),

    # local time
    (JoltLocalTime("12:00:00.000000001"), '{"T": "12:00:00.000000001"}'),
    (JoltLocalTime("12:00:00"), '{"T": "12:00:00"}'),
    (JoltLocalTime("12:00"), '{"T": "12:00"}'),

    # date time
    (
        JoltDateTime("2020-01-02T12:13:01.1234+01"),
        '{"T": "2020-01-02T12:13:01.1234+01"}'
    ),

    # local date time
    (
        JoltLocalDateTime("2020-01-02T12:13:01.1234"),
        '{"T": "2020-01-02T12:13:01.1234"}'
    ),

    # duration
    (JoltDuration("P1Y13M2DT18M"), '{"T": "P1Y13M2DT18M"}'),

    # point
    (JoltPoint("POINT(1 2)"), '{"@": "POINT(1 2)"}'),
    (JoltPoint("POINT (1 2)"), '{"@": "POINT (1 2)"}'),
    (JoltPoint("POINT(1 2.3)"), '{"@": "POINT(1 2.3)"}'),
    (JoltPoint("POINT(56.21 13.43)"), '{"@": "POINT(56.21 13.43)"}'),
    (JoltPoint("POINT(56.21 13.43 12)"), '{"@": "POINT(56.21 13.43 12)"}'),
    (JoltPoint("SRID=4326;POINT(1 2.3)"), '{"@": "SRID=4326;POINT(1 2.3)"}'),

    # node
    (
        JoltNode(123, ["l1", "l2"], {"a": 42}),
        '{"()": [123, ["l1", "l2"], {"a": 42}]}'
    ),
    (
        JoltNode(123, ["l1", "l2"], {"U": 42}),
        '{"()": [123, ["l1", "l2"], {"U": 42}]}'
    ),
    (
        JoltNode(123, ["l1", "l2"], {"U": 2147483648}),
        '{"()": [123, ["l1", "l2"], {"U": {"Z": "2147483648"}}]}'
    ),

    # relationship
    (
        JoltRelationship(42, 123, "REVERTS_TO", 321, {"prop": "value"}),
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": "value"}]}'
    ),

    # path
    (
        JoltPath(
            JoltNode(1, ["l"], {}),
            JoltRelationship(2, 1, "RELATES_TO", 3, {}),
            JoltNode(3, ["l"], {}),
            JoltRelationship(4, 3, "RELATES_TO", 1, {}),
            JoltNode(1, ["l"], {}),
        ),
        '{"..": ['
        '{"()": [1, ["l"], {}]}, '
        '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
        '{"()": [3, ["l"], {}]}, '
        '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
        '{"()": [1, ["l"], {}]}'
        ']}'
    ),
))
@pytest.mark.parametrize("human_readable", [True, False])
def test_dumps_simple(in_, out_, human_readable):
    if isinstance(out_, tuple):
        out_ = out_[human_readable]
    if inspect.isclass(out_) and issubclass(out_, BaseException):
        with pytest.raises(out_):
            dumps_simple(in_)
    else:
        assert dumps_simple(in_, human_readable=human_readable) == out_


@pytest.mark.parametrize(("in_", "out_"), (
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
    ('{"Z": "999999999999999999999999999999"}', 999999999999999999999999999999),

    # float - full
    ('{"R": "1.0"}', 1.0),
    ('{"R": "1.23456789"}', 1.23456789),
    ('{"R": "-1.23456789"}', -1.23456789),
    ('{"R": "0.0"}', 0.0),
    ('{"R": "-0.0"}', -0.0),
    ('{"R": "NaN"}', float("nan")),
    ('{"R": "+Infinity"}', float("inf")),
    ('{"R": "-Infinity"}', float("-inf")),

    # str - simple
    ('"abc"', "abc"),
    ('"ab\\u1234"', "ab\u1234"),

    # str - full
    ('{"U": "abc"}', "abc"),
    ('{"U": "ab\\u1234"}', "ab\u1234"),

    # bytes
    ('{"#": "1234"}', b"\x12\x34"),
    ('{"#": "12FF"}', b"\x12\xff"),
    ('{"#": "12 34"}', b"\x12\x34"),
    ('{"#": "12 FF"}', b"\x12\xff"),

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
    ('{}', {}),
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
        '{"T": "2020-01-02T12:13:01.1234+01"}',
        JoltDateTime("2020-01-02T12:13:01.1234+01")
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
    ('{"@": "POINT(56.21 13.43 12)"}', JoltPoint("POINT(56.21 13.43 12)")),
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
        ']}',
        JoltPath(
            JoltNode(1, ["l"], {}),
            JoltRelationship(2, 1, "RELATES_TO", 3, {}),
            JoltNode(3, ["l"], {}),
            JoltRelationship(4, 3, "RELATES_TO", 1, {}),
            JoltNode(1, ["l"], {}),
        )
    ),
    # TODO: test value errors (e.g. path with rels that don't match node ids)
))
def test_loads(in_, out_):
    def type_match(a, b):
        # assert a == b
        if isinstance(a, (list, tuple)):
            return all(type_match(ea, eb) for ea, eb in zip(a, b))
        if isinstance(a, dict):
            return (all(type_match(ka, kb)
                        for ka, kb in zip(sorted(a.keys()), sorted(b.keys())))
                    and all(type_match(a[k], b[k]) for k in a))
        return type(a) == type(b)

    res = loads(in_)
    if isinstance(out_, float) and math.isnan(out_):
        assert math.isnan(res)
    else:
        assert res == out_
    assert type_match(res, out_)
