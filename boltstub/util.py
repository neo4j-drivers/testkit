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


from copy import deepcopy
from threading import Lock


class EvalContext:
    def __init__(self):
        self._variables = {}
        self._lock = Lock()

    def __getitem__(self, item):
        with self._lock:
            return self._variables[item]

    def __setitem__(self, item, value):
        with self._lock:
            self._variables[item] = value

    def exec(self, cmd):
        with self._lock:
            exec(cmd, {}, self._variables)

    def eval(self, cmd, probing=False):
        with self._lock:
            locals_ = self._variables
            if probing:
                locals_ = deepcopy(locals_)
            return eval(cmd, {}, locals_)


def hex_repr(b, upper=True):
    if upper:
        return " ".join("{:02X}".format(x) for x in b)
    else:
        return " ".join("{:02x}".format(x) for x in b)


def recursive_subclasses(cls):
    for s_cls in cls.__subclasses__():
        yield s_cls
        for s_s_cls in recursive_subclasses(s_cls):
            yield s_s_cls
