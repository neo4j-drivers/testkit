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


from ..common.jolt_types import JoltType as JoltTypeCommon


class JoltType(JoltTypeCommon):  # version specific type base class
    pass


class JoltUuid(JoltType):
    """
    Represents a UUID value in JOLT v4.

    :param value: The UUID as a standard hyphenated string,
        e.g. "550e8400-e29b-41d4-a716-446655440000".
    """

    def __init__(self, value):
        self.value = str(value)

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if not isinstance(other, JoltUuid):
            return NotImplemented
        return self.value == other.value

    def __repr__(self):
        return f"{self.__class__.__name__}<{self.value!r}>"


__all__ = [
    "JoltType",
    "JoltUuid"
]
