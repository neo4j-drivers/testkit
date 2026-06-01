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


from uuid import UUID

import pytest

from ....simple_jolt.common.errors import JOLTValueError
from ....simple_jolt.v4 import (
    dumps_full,
    dumps_simple,
    loads,
)
from ... import _common
from ..v1.parse_data import V1_EXPLICIT_LOADS
from ..v2.parse_data import V2_EXPLICIT_LOADS
from ..v3.parse_data import V3_EXPLICIT_LOADS
from .parse_data import (
    V4_EXPLICIT_LOADS,
    V4_LOADS,
)


@pytest.mark.parametrize(("in_", "out_"), (
    # UUID
    (
        UUID("00000000-0000-0000-0000-000000000000"),
        '{"UU": "00000000-0000-0000-0000-000000000000"}',
    ),
    (
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        '{"UU": "ffffffff-ffff-ffff-ffff-ffffffffffff"}',
    ),
    (
        UUID("550e8400-e29b-41d4-a716-446655440000"),
        '{"UU": "550e8400-e29b-41d4-a716-446655440000"}',
    ),
    (
        UUID("01020304-0506-0708-090a-0b0c0d0e0f10"),
        '{"UU": "01020304-0506-0708-090a-0b0c0d0e0f10"}',
    ),
))
@pytest.mark.parametrize("human_readable", [True, False])
def test_dumps_full(in_, out_, human_readable):
    assert dumps_full(in_, human_readable=human_readable) == out_


@pytest.mark.parametrize(("in_", "out_"), (
    # UUID has no simple representation; falls through to full form
    (
        UUID("00000000-0000-0000-0000-000000000000"),
        '{"UU": "00000000-0000-0000-0000-000000000000"}',
    ),
    (
        UUID("550e8400-e29b-41d4-a716-446655440000"),
        '{"UU": "550e8400-e29b-41d4-a716-446655440000"}',
    ),
))
@pytest.mark.parametrize("human_readable", [True, False])
def test_dumps_simple(in_, out_, human_readable):
    assert dumps_simple(in_, human_readable=human_readable) == out_


@pytest.mark.parametrize(
    ("in_", "out_"),
    V4_LOADS
    + V4_EXPLICIT_LOADS
    + V3_EXPLICIT_LOADS
    + V2_EXPLICIT_LOADS
    + V1_EXPLICIT_LOADS,
)
def test_loads(in_, out_):
    res = loads(in_)
    assert _common.nan_and_type_equal(res, out_)


@pytest.mark.parametrize("in_", (
    # wrong value type
    '{"UU": 123}',
    '{"UU": 123.4}',
    '{"UU": true}',
    '{"UU": null}',
    '{"UU": []}',
    '{"UU": {}}',
    # malformed UUID string
    '{"UU": "not-a-uuid"}',
    '{"UU": "550e8400-e29b-41d4-a716-44665544000"}',   # one char short
    '{"UU": "550e8400-e29b-41d4-a716-4466554400000"}',  # one char long
    '{"UU": "550e8400e29b41d4a716446655440000"}',        # no hyphens
    '{"UU": ""}',
))
def test_verifies_uuid(in_):
    with pytest.raises(JOLTValueError):
        loads(in_)
