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


class JoltV3VectorMixin(JoltTypeCommon):

    DTYPE_SIZES = {
        "i8": 1,
        "i16": 2,
        "i32": 4,
        "i64": 8,
        "f32": 4,
        "f64": 8,
    }

    def __init__(self, dtype, data):
        self.dtype = dtype
        self.data = data

    def __eq__(self, other):
        if not isinstance(other, JoltV3VectorMixin):
            return NotImplemented

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("dtype", "data"))

    def __repr__(self):
        cls_name = self.__class__.__name__
        dtype = self.dtype
        data = self.data
        return f"{cls_name}<{dtype!r}, {data!r}>"


class JoltVector(JoltV3VectorMixin, JoltType):
    """
    Represents a vector type in JOLT v3.

    :param dtype: The data type of the vector elements.
    :param data: The raw data of the vector.
    """

    def __init__(self, dtype, data):
        super().__init__(dtype, data)


class JoltUnsupportedType(JoltType):
    """
    Represents an Unsupported Type object in Jolt v3.

    :param name: The name of the type that could not be sent.
    :param minimum_protocol_major: The major bolt version needed for the type.
    :param minimum_protocol_minor: The minor bolt version needed for the type.
    :param message: The optional message to the user.
    """

    def __init__(
        self, name, minimum_protocol_major,
        minimum_protocol_minor, message=None
    ):
        self.name = name
        self.minimum_protocol_major = minimum_protocol_major
        self.minimum_protocol_minor = minimum_protocol_minor
        self.message = message

    def __eq__(self, other):
        if not isinstance(other, JoltUnsupportedType):
            return NotImplemented

        return all(
            getattr(self, attr) == getattr(other, attr)
            for attr in (
                "name", "minimum_protocol_major",
                "minimum_protocol_major", "message"
            )
        )

    def __repr__(self):
        cls_name = self.__class__.__name__
        name = self.name
        minimum_protocol_major = self.minimum_protocol_major
        minimum_protocol_minor = self.minimum_protocol_minor
        message = self.message
        return (
            f"{cls_name}<{name!r}, {minimum_protocol_major!r}, "
            f"{minimum_protocol_minor!r}, {message!r} >"
        )


__all__ = [
    JoltType,
    JoltUnsupportedType,
    JoltVector,
]
