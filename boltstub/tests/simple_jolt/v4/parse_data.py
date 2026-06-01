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
from uuid import UUID


V4_LOADS = (
    # UUID - full
    (
        '{"UU": "00000000-0000-0000-0000-000000000000"}',
        UUID("00000000-0000-0000-0000-000000000000"),
    ),
    (
        '{"UU": "ffffffff-ffff-ffff-ffff-ffffffffffff"}',
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ),
    (
        '{"UU": "550e8400-e29b-41d4-a716-446655440000"}',
        UUID("550e8400-e29b-41d4-a716-446655440000"),
    ),
    (
        '{"UU": "01020304-0506-0708-090a-0b0c0d0e0f10"}',
        UUID("01020304-0506-0708-090a-0b0c0d0e0f10"),
    ),
)


def _get_v4_explicit_loads():
    res = []
    for in_, out in V4_LOADS:
        loaded_in = json.loads(in_)
        if isinstance(loaded_in, dict) and set(loaded_in.keys()) == {"UU"}:
            key = next(iter(loaded_in.keys()))
            loaded_in[key + "v4"] = loaded_in.pop(key)
            res.append((json.dumps(loaded_in), out))
    return tuple(res)


V4_EXPLICIT_LOADS = _get_v4_explicit_loads()
