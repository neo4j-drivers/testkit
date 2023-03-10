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


import re

from .errors import JOLTValueError


class JoltType:
    pass


class JoltWildcard(JoltType):
    """
    This is a stub-server specific JOLT type that marks a match-all object.

    e.g. `{"Z": "*"}` represents any integer.
    """

    def __init__(self, types):
        self.types = types


class _JoltParsedType(JoltType):
    # to be overridden in subclasses (this re never matches)
    _parse_re = re.compile(r"^(?= )$")

    def __init__(self, value: str):
        match = self._parse_re.match(value)
        if not match:
            raise JOLTValueError(
                "{} didn't match the types format: {}".format(
                    value, self._parse_re
                )
            )
        self._str = value
        self._groups = match.groups()

    def __str__(self):
        return self._str


__all__ = [
    JoltType,
    JoltWildcard,
]
