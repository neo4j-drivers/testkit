import inspect

import pytest

from .. import _common
from ...simple_jolt import (
    dumps_full,
    dumps_simple,
    loads,
)
from ...simple_jolt.errors import JOLTValueError
from ...simple_jolt.types import (
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
    (
        999999999999999999999999999999,
        '{"Z": "999999999999999999999999999999"}'
    ),

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
        ']}'  # noqa: Q000
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
    (
        999999999999999999999999999999,
        '{"Z": "999999999999999999999999999999"}'
    ),

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
    ((1,), "[1]"),
    (("a",), '["a"]'),
    ((1, "a"), '[1, "a"]'),
    ((1, ["a"]), '[1, ["a"]]'),

    # dict
    ({}, '{"{}": {}}'),
    ({"a": 1}, '{"{}": {"a": 1}}'),
    ({"#": "no bytes"}, '{"{}": {"#": "no bytes"}}'),
    ({1: 1}, JOLTValueError),

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
        ']}'  # noqa: Q000
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
))
def test_loads(in_, out_):
    res = loads(in_)
    assert _common.nan_and_type_equal(res, out_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"?": 123}',
    '{"?": 123.4}',
    '{"?": [123]}',
    '{"?": [123.4]}',
    '{"?": {"?": true}}',
    '{"?": "true"}',
    '{"?": {"a": "b"}}',
))
def test_verifies_bool(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"Z": 123}',
    '{"Z": 123.4}',
    '{"Z": [123]}',
    '{"Z": [123.4]}',
    '{"Z": {"Z": "123"}}',
    '{"Z": true}',
    # garbage str repr
    '{"Z": "abd"}',
    '{"Z": "123a"}',
    '{"Z": "123,5"}',
    '{"Z": "123.a"}',
))
def test_verifies_int(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"R": 123}',
    '{"R": 123.4}',
    '{"R": [123]}',
    '{"R": [123.4]}',
    '{"R": {"R": "123.4"}}',
    '{"R": true}',
    # garbage str repr
    '{"R": "abd"}',
    '{"R": "123a"}',
    '{"R": "123,5"}',
))
def test_verifies_float(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"U": 123}',
    '{"U": 123.4}',
    '{"U": [123]}',
    '{"U": true}',
    '{"U": {"a": "b"}}',
    '{"U": {"U": "b"}}',
))
def test_verifies_str(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"#": 123.4}',
    '{"#": 123}',
    '{"#": {"a": 123}}',
    '{"#": true}',
    # invalid string repr
    '{"#": "123"}',
    '{"#": "FG"}',
    '{"#": "F F FF"}',
    '{"#": "[1, 2]"}',
    # invalid list repr
    '{"#": [-1, 128]}',
    '{"#": [128.0, 128]}',
    '{"#": [128, 256]}',
    '{"#": ["128", 255]}',
))
def test_verifies_bytes(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # not a list value
    '{"[]": 123.4}',
    '{"[]": 123}',
    '{"[]": "a"}',
    '{"[]": true}',
    '{"[]": {"a": 1}}',
    '{"[]": "[1, 2]"}',
))
def test_verifies_list(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # not a dict value
    '{"{}": 123.4}',
    '{"{}": 123}',
    '{"{}": "a"}',
    '{"{}": true}',
    r'{"{}": "{\"a\": \"b\"}"}',
))
def test_verifies_dict(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # lower case time separator(
    '{"T": "2020-01-02t12:13:01.1234+01"}',
    # too high precision
    '{"T": "2020-01-02T12:13:01.1234567890+01"}',
    # garbage
    '{"T": "123456"}',
    # int value
    '{"T": 12345}',
    # bool value
    '{"T": true}',
    # lower case duration marker
    '{"T": "p1Y13M2DT18M"}',
    '{"T": "P1Y13m2DT18M"}',
))
def test_verifies_duration(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # str srid
    r'{"@": "SRID=\"4326\";POINT(1 2.3)"}',
    # comma srid separator
    '{"@": "SRID=4326,POINT(1 2.3)"}',
    # negative srid
    '{"@": "SRID=-4326;POINT(1 2.3)"}',
    # 4D point
    '{"@": "SRID=4326;POINT(1 2.3 3 4.1234)"}',
    # Wrong keyword
    '{"@": "SRID=4326;POiNT(1 2.3)"}',
    # missing parentheses
    '{"@": "SRID=4326;POINT(1 2.3"}',
    # extra parentheses
    '{"@": "SRID=4326;POINT(1 2.3))"}',
    # comma separated arguments
    '{"@": "SRID=4326;POINT(1, 2.3)"}',
    # bool value
    '{"@": true}',
    # list value
    '{"@": [1, 2]}',
))
def test_verifies_point(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # str id
    '{"()": ["1", ["l"], {}]}',
    # int label
    '{"()": [1, [1], {}]}',
    # str label
    '{"()": [1, "[]", {}]}',
    # list property
    '{"()": [1, ["l"], ["a", "b"]]}',
))
def test_verifies_node(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # dict id
    '{"->": [{"U": "2"}, 1, "RELATES_TO", 3, {}]}',
    # str dict
    '{"->": ["2", 1, "RELATES_TO", 3, {}]}',
    # str start id
    '{"->": [2, "1", "RELATES_TO", 3, {}]}',
    # int label
    '{"->": [2, 1, 1337, 3, {}]}',
    # str end id
    '{"->": [2, 1, "RELATES_TO", "3", {}]}',
    # list properties
    '{"->": [2, 1, "RELATES_TO", "3", ["a", "b"]]}',
))
@pytest.mark.parametrize("flip", [True, False])
def test_verifies_relationship(in_, flip):
    if flip:
        assert in_[:6] == '{"->":'
        in_ = '{"<-":' + in_[6:]
    with pytest.raises(JOLTValueError):
        loads(in_)


@pytest.mark.parametrize("in_", (
    # Link to non-existent node
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 4, {}]}, '
    '{"()": [3, ["l"], {}]}, '
    '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}'
    ']}',  # noqa: Q000

    # Link from non-existent node
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 4, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}, '
    '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}'
    ']}',  # noqa: Q000

    # double link
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"->": [4, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}'
    ']}',  # noqa: Q000
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"->": [4, 1, "RELATES_TO", 3, {}]}, '
    '{"->": [5, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}'
    ']}',  # noqa: Q000

    # double node
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"()": [4, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [4, ["l"], {}]} '
    ']}',  # noqa: Q000
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"()": [4, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [5, ["l"], {}]}, '
    '{"()": [4, ["l"], {}]} '
    ']}',  # noqa: Q000

    # only nodes
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"()": [4, ["l"], {}]}, '
    '{"()": [2, ["l"], {}]}'
    ']}',  # noqa: Q000

    # only relationships
    '{"..": ['
    '{"->": [1, 1, "RELATES_TO", 3, {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"->": [3, 1, "RELATES_TO", 3, {}]}'
    ']}',  # noqa: Q000

    # start with relationship
    '{"..": ['
    '{"->": [5, 1, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}, '
    '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}'
    ']}',  # noqa: Q000

    # ending with relationship
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}, '
    '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}, '
    '{"->": [5, 1, "RELATES_TO", 1, {}]}'
    ']}',  # noqa: Q000
    '{"..": ['
    '{"()": [1, ["l"], {}]}, '
    '{"->": [2, 1, "RELATES_TO", 3, {}]}, '
    '{"()": [3, ["l"], {}]}, '
    '{"->": [4, 3, "RELATES_TO", 1, {}]}, '
    '{"()": [1, ["l"], {}]}, '
    '{"->": [6, 1, "RELATES_TO", 1, {}]}, '
    '{"->": [6, 1, "RELATES_TO", 1, {}]}'
    ']}',  # noqa: Q000

    # total bogus
    '{"..": {"Z": "1234"}}'
))
def test_verifies_path(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)
