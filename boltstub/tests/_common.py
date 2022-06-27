import math
from itertools import cycle

from ..bolt_protocol import Structure
from ..simple_jolt import v1 as jolt_v1
from ..simple_jolt import v2 as jolt_v2


def cycle_zip(*args):
    max_len_idx = max(range(len(args)), key=lambda i: len(args[i]))
    return zip(*(arg if i == max_len_idx else cycle(arg)
               for i, arg in enumerate(args)))


ALL_SERVER_VERSIONS = (1,), (2,), (3,), (4, 1), (4, 2), (4, 3)


ALL_REQUESTS_PER_VERSION = tuple((
    *(
        ((major,), tag, name)
        for major in (1, 2)
        for (tag, name) in (
            (b"\x01", "INIT"),
            (b"\x0E", "ACK_FAILURE"),
            (b"\x0F", "RESET"),
            (b"\x10", "RUN"),
            (b"\x2F", "DISCARD_ALL"),
            (b"\x3F", "PULL_ALL"),
        )
    ),

    ((3,), b"\x01", "HELLO"),
    ((3,), b"\x02", "GOODBYE"),
    ((3,), b"\x0F", "RESET"),
    ((3,), b"\x10", "RUN"),
    ((3,), b"\x2F", "DISCARD_ALL"),
    ((3,), b"\x3F", "PULL_ALL"),
    ((3,), b"\x11", "BEGIN"),
    ((3,), b"\x12", "COMMIT"),
    ((3,), b"\x13", "ROLLBACK"),

    *(
        ((4, minor), tag, name)
        for minor in range(4)
        for (tag, name) in (
            (b"\x01", "HELLO"),
            (b"\x02", "GOODBYE"),
            (b"\x0F", "RESET"),
            (b"\x10", "RUN"),
            (b"\x2F", "DISCARD"),
            (b"\x3F", "PULL"),
            (b"\x11", "BEGIN"),
            (b"\x12", "COMMIT"),
            (b"\x13", "ROLLBACK"),
        )
    )
))


ALL_RESPONSES_PER_VERSION = tuple((*(
    (version, tag, name)
    for version in ALL_SERVER_VERSIONS
    for (tag, name) in (
        (b"\x70", "SUCCESS"),
        (b"\x7E", "IGNORED"),
        (b"\x7F", "FAILURE"),
        (b"\x71", "RECORD"),
    )
),))


ALL_PACKSTREAM_VERSIONS = (1, 2)


def get_jolt_package(version):
    if version == 1:
        return jolt_v1
    elif version == 2:
        return jolt_v2
    else:
        raise ValueError("Unknown jolt version %s" % version)


_JOLT_FIELD_REPR_TO_FIELDS_V1_V2 = (
    # none
    ("null", [None]),

    # bool - simple
    ("true", [True]),
    ("false", [False]),

    # bool - full
    ('{"?": true}', [True]),
    ('{"?": false}', [False]),

    # int/float - simple
    ("1", [1]),
    ("0", [0]),
    ("-1", [-1]),
    ("-2147483648", [-2147483648]),
    ("-2147483649", [-2147483649.]),
    ("2147483647", [2147483647]),
    ("2147483648", [2147483648.]),

    # int - full
    ('{"Z": "1"}', [1]),
    ('{"Z": "0"}', [0]),
    ('{"Z": "-1"}', [-1]),
    ('{"Z": "-2147483648"}', [-2147483648]),
    ('{"Z": "-2147483649"}', [-2147483649]),
    ('{"Z": "2147483647"}', [2147483647]),
    ('{"Z": "2147483648"}', [2147483648]),

    # float - full
    ('{"R": "1.0"}', [1.0]),
    ('{"R": "1.23456789"}', [1.23456789]),
    ('{"R": "-1.23456789"}', [-1.23456789]),
    ('{"R": "0.0"}', [0.0]),
    ('{"R": "-0.0"}', [-0.0]),
    ('{"R": "NaN"}', [float("nan")]),
    ('{"R": "+Infinity"}', [float("inf")]),
    ('{"R": "-Infinity"}', [float("-inf")]),

    # str - simple
    ('"abc"', ["abc"]),

    # str - full
    ('{"U": "abc"}', ["abc"]),

    # bytes
    ('{"#": "1234"}', [b"\x12\x34"]),
    ('{"#": "12FF"}', [b"\x12\xff"]),
    ('{"#": "12 34"}', [b"\x12\x34"]),
    ('{"#": "12 FF"}', [b"\x12\xff"]),

    # list - simple
    ("[]", [[]]),
    ("[1]", [[1]]),
    ('["a"]', [["a"]]),
    ('[1, "a"]', [[1, "a"]]),
    ('[1, ["a"]]', [[1, ["a"]]]),

    # list - full
    ('{"[]": []}', [[]]),
    ('{"[]": [{"Z": "1"}]}', [[1]]),
    ('{"[]": [{"U": "a"}]}', [["a"]]),
    ('{"[]": [{"Z": "1"}, {"U": "a"}]}', [[1, "a"]]),
    ('{"[]": [{"Z": "1"}, {"[]": [{"U": "a"}]}]}', [[1, ["a"]]]),

    # dict - full
    ('{"{}": {}}', [{}]),
    ('{"{}": {"a": 1}}', [{"a": 1}]),
    ('{"{}": {"#": "no bytes"}}', [{"#": "no bytes"}]),
)

_JOLT_FIELD_REPR_TO_FIELDS_V1 = (
    # date - full
    ('{"T": "1970-01-02"}', [Structure(b"\x44", 1, packstream_version=1)]),

    # time - full
    ('{"T": "00:00:02Z"}', [Structure(b"\x54", 2000000000, 0,
                                      packstream_version=1)]),
    ('{"T": "00:00:02.001+0130"}', [Structure(b"\x54", 2001000000, 5400,
                                              packstream_version=1)]),

    # local time - full
    ('{"T": "00:01:00.000000001"}', [Structure(b"\x74", 60000000001,
                                               packstream_version=1)]),
    ('{"T": "01:00:01"}', [Structure(b"\x74", 3601000000000,
                                     packstream_version=1)]),

    # date time - full
    (
        '{"T": "1970-01-02T01:01:01.1234+01"}',
        [Structure(b"\x46", 90061, 123400000, 3600, packstream_version=1)]
    ),
    (
        '{"T": "1970-01-02T01:01:01.1234+01[Europe/Stockholm]"}',
        [Structure(b"\x66", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=1)]
    ),
    (  # packstream v1 bug: non-unique times
        '{"T": "1970-10-25T01:30-04:00[America/New_York]"}',
        [Structure(b"\x66", 25666200, 0,
                   "America/New_York", packstream_version=1)]
    ),
    (  # Wow! The same encoding \o/ [/sarcasm]
        '{"T": "1970-10-25T01:30-05:00[America/New_York]"}',
        [Structure(b"\x66", 25666200, 0,
                   "America/New_York", packstream_version=1)]
    ),

    # local date time - full
    (
        '{"T": "1970-01-02T01:01:01.000001234"}',
        [Structure(b"\x64", 90061, 1234, packstream_version=1)]
    ),

    # duration - full
    (
        '{"T": "P1Y13M2DT18M0.1S"}',
        [Structure(b"\x45", 25, 2, 1080, 100000000, packstream_version=1)]
    ),

    # point - full
    ('{"@": "SRID=123;POINT(1 2.3)"}', [Structure(b"\x58", 123, 1.0, 2.3,
                                                  packstream_version=1)]),
    (
        '{"@": "SRID=42;POINT(56.21 13.43 12)"}',
        [Structure(b"\x59", 42, 56.21, 13.43, 12., packstream_version=1)]
    ),

    # node
    (
        '{"()": [123, ["l1", "l2"], {"a": 42}]}',
        [Structure(b"\x4E", 123, ["l1", "l2"], {"a": 42},
                   packstream_version=1)]
    ),
    (
        '{"()": [123, ["l1", "l2"], {"U": {"Z": "2147483648"}}]}',
        [Structure(b"\x4E", 123, ["l1", "l2"], {"U": 2147483648},
                   packstream_version=1)]
    ),

    # relationship
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": "value"}]}',
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   packstream_version=1)]
    ),
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": {"U": "value"}}]}',
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   packstream_version=1)]
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
        [Structure(
            b"\x50",
            [  # Nodes
                Structure(b"\x4E", 1, ["l"], {}, packstream_version=1),
                Structure(b"\x4E", 3, ["l"], {}, packstream_version=1),
            ],
            [  # Relationships
                Structure(b"\x72", 2, "RELATES_TO", {}, packstream_version=1),
                Structure(b"\x72", 4, "RELATES_TO", {}, packstream_version=1),
            ],
            [1, 1, 2, 0],  # idxs
            packstream_version=1
        )]
    ),
)

_JOLT_FIELD_REPR_TO_FIELDS_V1_V2_COMPAT = (
    # date v2 - full
    ('{"Tv2": "1970-01-02"}', [Structure(b"\x44", 1, packstream_version=2)]),

    # time v2 - full
    ('{"Tv2": "00:00:02Z"}', [Structure(b"\x54", 2000000000, 0,
                                        packstream_version=2)]),
    ('{"Tv2": "00:00:02.001+0130"}', [Structure(b"\x54", 2001000000, 5400,
                                                packstream_version=2)]),

    # local time v2 - full
    ('{"Tv2": "00:01:00.000000001"}', [Structure(b"\x74", 60000000001,
                                                 packstream_version=2)]),
    ('{"Tv2": "01:00:01"}', [Structure(b"\x74", 3601000000000,
                                       packstream_version=2)]),

    # date time v2 - full
    (
        '{"Tv2": "1970-01-02T01:01:01.1234+01"}',
        [Structure(b"\x49", 90061 - 3600, 123400000, 3600,
                   packstream_version=2)]
    ),
    (
        '{"Tv2": "1970-01-02T02:01:01.1234+01[Europe/Stockholm]"}',
        [Structure(b"\x69", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=2)]
    ),
)

_JOLT_FIELD_REPR_TO_FIELDS_V2 = (
    # date - full
    ('{"T": "1970-01-02"}', [Structure(b"\x44", 1, packstream_version=2)]),

    # time - full
    ('{"T": "00:00:02Z"}', [Structure(b"\x54", 2000000000, 0,
                                      packstream_version=2)]),
    ('{"T": "00:00:02.001+0130"}', [Structure(b"\x54", 2001000000, 5400,
                                              packstream_version=2)]),

    # local time - full
    ('{"T": "00:01:00.000000001"}', [Structure(b"\x74", 60000000001,
                                               packstream_version=2)]),
    ('{"T": "01:00:01"}', [Structure(b"\x74", 3601000000000,
                                     packstream_version=2)]),

    # date time - full
    (
        '{"T": "1970-01-02T01:01:01.1234+01"}',
        [Structure(b"\x49", 90061 - 3600, 123400000, 3600,
                   packstream_version=2)]
    ),
    (
        '{"T": "1970-01-02T02:01:01.1234+01[Europe/Stockholm]"}',
        [Structure(b"\x69", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=2)]
    ),
    (  # packstream v1 bug (non-unique times) fixed
        '{"T": "1970-10-25T01:30-04:00[America/New_York]"}',
        [Structure(b"\x69", 25680600, 0,
                   "America/New_York", packstream_version=2)]
    ),
    (  # Wow! Different times have different representations :O
        '{"T": "1970-10-25T01:30-05:00[America/New_York]"}',
        [Structure(b"\x69", 25684200, 0,
                   "America/New_York", packstream_version=2)]
    ),

    # local date time - full
    (
        '{"T": "1970-01-02T01:01:01.000001234"}',
        [Structure(b"\x64", 90061, 1234, packstream_version=2)]
    ),

    # duration - full
    (
        '{"T": "P1Y13M2DT18M0.1S"}',
        [Structure(b"\x45", 25, 2, 1080, 100000000, packstream_version=2)]
    ),

    # point - full
    ('{"@": "SRID=123;POINT(1 2.3)"}', [Structure(b"\x58", 123, 1.0, 2.3,
                                                  packstream_version=2)]),
    (
        '{"@": "SRID=42;POINT(56.21 13.43 12)"}',
        [Structure(b"\x59", 42, 56.21, 13.43, 12., packstream_version=2)]
    ),

    # node
    (
        '{"()": [123, ["l1", "l2"], {"a": 42}, "123"]}',
        [Structure(b"\x4E", 123, ["l1", "l2"], {"a": 42}, "123",
                   packstream_version=2)]
    ),
    (
        '{"()": [null, ["l1", "l2"], {"a": 42}, "123"]}',
        [Structure(b"\x4E", None, ["l1", "l2"], {"a": 42}, "123",
                   packstream_version=2)]
    ),
    (
        '{"()": [123, ["l1", "l2"], {"U": {"Z": "2147483648"}}, "123"]}',
        [Structure(b"\x4E", 123, ["l1", "l2"], {"U": 2147483648}, "123",
                   packstream_version=2)]
    ),

    # relationship
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": "value"}, '
        '"42", "123", "321"]}',
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   "42", "123", "321", packstream_version=2)]
    ),
    (
        '{"->": [42, 123, "REVERTS_TO", 321, {"prop": {"U": "value"}}, '
        '"42", "123", "321"]}',
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   "42", "123", "321", packstream_version=2)]
    ),

    # path
    (
        '{"..": ['
        '{"()": [1, ["l"], {}, "1"]}, '
        '{"->": [2, 1, "RELATES_TO", 3, {}, "2", "1", "3"]}, '
        '{"()": [3, ["l"], {}, "3"]}, '
        '{"->": [4, 3, "RELATES_TO", 1, {}, "4", "3", "1"]}, '
        '{"()": [1, ["l"], {}, "1"]}'
        ']}',  # noqa: Q000
        [Structure(
            b"\x50",
            [  # Nodes
                Structure(b"\x4E", 1, ["l"], {}, "1", packstream_version=2),
                Structure(b"\x4E", 3, ["l"], {}, "3", packstream_version=2),
            ],
            [  # Relationships
                Structure(b"\x72", 2, "RELATES_TO", {}, "2",
                          packstream_version=2),
                Structure(b"\x72", 4, "RELATES_TO", {}, "4",
                          packstream_version=2),
            ],
            [1, 1, 2, 0],  # idxs
            packstream_version=2
        )]
    ),
)

_JOLT_FIELD_REPR_TO_FIELDS_V2_V1_COMPAT = (
    # date v1 - full
    ('{"Tv1": "1970-01-02"}', [Structure(b"\x44", 1, packstream_version=1)]),

    # time v1 - full
    ('{"Tv1": "00:00:02Z"}', [Structure(b"\x54", 2000000000, 0,
                                        packstream_version=1)]),
    ('{"Tv1": "00:00:02.001+0130"}', [Structure(b"\x54", 2001000000, 5400,
                                                packstream_version=1)]),

    # local time v1 - full
    ('{"Tv1": "00:01:00.000000001"}', [Structure(b"\x74", 60000000001,
                                                 packstream_version=1)]),
    ('{"Tv1": "01:00:01"}', [Structure(b"\x74", 3601000000000,
                                       packstream_version=1)]),

    # date time v1 - full
    (
        '{"Tv1": "1970-01-02T01:01:01.1234+01"}',
        [Structure(b"\x46", 90061, 123400000, 3600, packstream_version=1)]
    ),
    (
        '{"Tv1": "1970-01-02T01:01:01.1234+01[Europe/Stockholm]"}',
        [Structure(b"\x66", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=1)]
    ),
)

JOLT_FIELD_REPR_TO_FIELDS = (
    *((*ex, 1) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V1_V2),
    *((*ex, 2) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V1_V2),
    *((*ex, 1) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V1),
    *((*ex, 1) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V1_V2_COMPAT),
    *((*ex, 2) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V2),
    *((*ex, 2) for ex in _JOLT_FIELD_REPR_TO_FIELDS_V2_V1_COMPAT),
)

_JOLT_WILDCARD_TO_FIELDS_V1_V2 = (
    # bool
    ('{"?": "*"}', (
        [True],
        [False]
    )),

    # int
    ('{"Z": "*"}', (
        [1],
        [0],
        [-1],
        [-2147483648],
        [-2147483649],
        [2147483647],
        [2147483648],
    )),

    # float
    ('{"R": "*"}', (
        [1.0],
        [1.23456789],
        [-1.23456789],
        [0.0],
        [-0.0],
        [float("nan")],
        [float("inf")],
        [float("-inf")],
    )),

    # str
    ('{"U": "*"}', (
        [""],
        ["abc"],
        ["*"],
        ["\u1234foobar"],
    )),

    # bytes
    ('{"#": "*"}', (
        [b"\x12\x34"],
        [b""],
        [b"\x00"],
    )),

    # list
    ('{"[]": "*"}', (
        [[]],
        [[1]],
        [["a"]],
        [[1, "a"]],
        [[1, ["a"]]],
    )),

    # dict
    ('{"{}": "*"}', (
        [{}],
        [{"a": 1}],
        [{"#": "no bytes"}],
    )),
)

_JOLT_WILDCARD_TO_FIELDS_V1 = (
    # date / time / local time / date time / local date time / duration
    ('{"T": "*"}', (
        [Structure(b"\x44", 1, packstream_version=1)],
        [Structure(b"\x54", 2000000000, 0, packstream_version=1)],
        [Structure(b"\x54", 2001000000, 5400, packstream_version=1)],
        [Structure(b"\x74", 60000000001, packstream_version=1)],
        [Structure(b"\x74", 3601000000000, packstream_version=1)],
        [Structure(b"\x46", 90061, 123400000, 3600, packstream_version=1)],
        [Structure(b"\x66", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=1)],
        [Structure(b"\x64", 90061, 1234, packstream_version=1)],
        [Structure(b"\x45", 25, 2, 1080, 100000000, packstream_version=1)],
    )),

    # point 2D / point 3D
    ('{"@": "*"}', (
        [Structure(b"\x58", 123, 1.0, 2.3, packstream_version=1)],
        [Structure(b"\x59", 42, 56.21, 13.43, 12., packstream_version=1)],
    )),

    # node
    ('{"()": "*"}', (
        [Structure(b"\x4E", 123, ["l1", "l2"], {"a": 42},
                   packstream_version=1)],
        [Structure(b"\x4E", 0, [], {}, packstream_version=1)],
    )),

    # relationship
    ('{"->": "*"}', (
        [Structure(b"\x52", 0, 0, 0, "", {}, packstream_version=1)],
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   packstream_version=1)],
    )),

    # path
    ('{"..": "*"}', (
        [Structure(b"\x50", [], [], [], packstream_version=1)],
        [Structure(
            b"\x50",
            [  # Nodes
                Structure(b"\x4E", 1, ["l"], {}, packstream_version=1),
                Structure(b"\x4E", 3, ["l"], {}, packstream_version=1),
            ],
            [  # Relationships
                Structure(b"\x72", 2, "RELATES_TO", {}, packstream_version=1),
                Structure(b"\x72", 4, "RELATES_TO", {}, packstream_version=1),
            ],
            [2, 1, 2, 0],  # idxs
            packstream_version=1
        )],
    )),
)

_JOLT_WILDCARD_TO_FIELDS_V2 = (
    # date / time / local time / date time / local date time / duration
    ('{"T": "*"}', (
        [Structure(b"\x44", 1, packstream_version=2)],
        [Structure(b"\x54", 2000000000, 0, packstream_version=2)],
        [Structure(b"\x54", 2001000000, 5400, packstream_version=2)],
        [Structure(b"\x74", 60000000001, packstream_version=2)],
        [Structure(b"\x74", 3601000000000, packstream_version=2)],
        [Structure(b"\x49", 90061, 123400000, 3600, packstream_version=2)],
        [Structure(b"\x69", 90061, 123400000, "Europe/Stockholm",
                   packstream_version=2)],
        [Structure(b"\x64", 90061, 1234, packstream_version=2)],
        [Structure(b"\x45", 25, 2, 1080, 100000000, packstream_version=2)],
    )),

    # point 2D / point 3D
    ('{"@": "*"}', (
        [Structure(b"\x58", 123, 1.0, 2.3, packstream_version=2)],
        [Structure(b"\x59", 42, 56.21, 13.43, 12., packstream_version=2)],
    )),

    # node
    ('{"()": "*"}', (
        [Structure(b"\x4E", 123, ["l1", "l2"], {"a": 42}, "123",
                   packstream_version=2)],
        [Structure(b"\x4E", 0, [], {}, "0", packstream_version=2)],
    )),

    # relationship
    ('{"->": "*"}', (
        [Structure(b"\x52", 0, 0, 0, "", {}, "0", "0", "0",
                   packstream_version=2)],
        [Structure(b"\x52", None, None, None, "", {}, "0", "0", "0",
                   packstream_version=2)],
        [Structure(b"\x52", 42, 123, 321, "REVERTS_TO", {"prop": "value"},
                   "42", "123", "321", packstream_version=2)],
    )),

    # path
    ('{"..": "*"}', (
        [Structure(b"\x50", [], [], [], packstream_version=2)],
        [Structure(
            b"\x50",
            [  # Nodes
                Structure(b"\x4E", 1, ["l"], {}, "1", packstream_version=2),
                Structure(b"\x4E", 3, ["l"], {}, "3", packstream_version=2),
            ],
            [  # Relationships
                Structure(b"\x72", 2, "RELATES_TO", {}, "2",
                          packstream_version=2),
                Structure(b"\x72", 4, "RELATES_TO", {}, "4",
                          packstream_version=2),
            ],
            [2, 1, 2, 0],  # idxs
            packstream_version=2
        )],
    )),
)

JOLT_WILDCARD_TO_FIELDS = (
    *((*ex, 1) for ex in _JOLT_WILDCARD_TO_FIELDS_V1_V2),
    *((*ex, 2) for ex in _JOLT_WILDCARD_TO_FIELDS_V1_V2),
    *((*ex, 1) for ex in _JOLT_WILDCARD_TO_FIELDS_V1),
    *((*ex, 2) for ex in _JOLT_WILDCARD_TO_FIELDS_V2),
)


def nan_and_type_equal(a, b):
    if isinstance(a, list):
        if not isinstance(b, list) or len(a) != len(b):
            return False
        return all(nan_and_type_equal(a_, b_) for a_, b_ in zip(a, b))
    if isinstance(a, tuple):
        if not isinstance(b, tuple) or len(a) != len(b):
            return False
        return all(nan_and_type_equal(a_, b_) for a_, b_ in zip(a, b))
    if isinstance(a, dict):
        if not all(isinstance(k, str) for k in a.keys()):
            raise NotImplementedError("Only sting-only-key dicts supported")
        if not isinstance(b, dict):
            return False
        if not all(isinstance(k, str) for k in b.keys()):
            raise NotImplementedError("Only sting-only-key dicts supported")
        return (set(a.keys()) == set(b.keys())
                and all(nan_and_type_equal(a[k], b[k]) for k in a.keys()))
    if isinstance(a, float):
        if not isinstance(b, float):
            return False
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b
    if isinstance(a, int):
        return isinstance(b, int) and a == b
    return a == b
