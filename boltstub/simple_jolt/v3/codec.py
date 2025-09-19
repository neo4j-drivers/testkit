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


import inspect

from ..common.errors import (
    JOLTValueError,
    NoSimpleRepresentation,
)
from ..v1.codec import (
    encode_bytes,
    parse_bytes,
)

# unused transformer imports are required for the codec to pick them up
from ..v2.codec import Codec as _Codec
from ..v2.codec import JoltBoolTransformer  # noqa: F401
from ..v2.codec import JoltBytesTransformer  # noqa: F401
from ..v2.codec import JoltDateTimeTransformer  # noqa: F401
from ..v2.codec import JoltDictTransformer  # noqa: F401
from ..v2.codec import JoltFloatTransformer  # noqa: F401
from ..v2.codec import JoltIntTransformer  # noqa: F401
from ..v2.codec import JoltListTransformer  # noqa: F401
from ..v2.codec import JoltNodeTransformer  # noqa: F401
from ..v2.codec import JoltNullTransformer  # noqa: F401
from ..v2.codec import JoltPathTransformer  # noqa: F401
from ..v2.codec import JoltPointTransformer  # noqa: F401
from ..v2.codec import JoltRelationTransformer  # noqa: F401
from ..v2.codec import JoltReverseRelationTransformer  # noqa: F401
from ..v2.codec import JoltStrTransformer  # noqa: F401
from ..v2.codec import JoltTypeTransformer
from .jolt_types import (
    JoltUnsupportedType,
    JoltVector,
)


class JoltVectorTransformer(JoltTypeTransformer):
    _supported_types = (JoltVector,)
    sigil = "V"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list):
            raise JOLTValueError('Expecting array after sigil "V"')
        if len(value) != 2:
            raise JOLTValueError('Expecting array of length 2 after sigil "V"')
        if not isinstance(value[0], str):
            raise JOLTValueError(
                "Expecting vector type as string as first element of array "
                'after sigil "V"'
            )
        dtype = value[0]
        if dtype not in JoltVector.DTYPE_SIZES:
            raise JOLTValueError(
                f"Unknown vector dtype {dtype!r}, supported dtypes are "
                f"{list(JoltVector.DTYPE_SIZES.keys())}"
            )
        try:
            data = parse_bytes(value[1])
        except ValueError as e:
            raise JOLTValueError(f"{e} as data after sigil 'V'") from None
        if len(data) % JoltVector.DTYPE_SIZES[dtype] != 0:
            raise JOLTValueError(
                f"Data length {len(data)} is not a multiple of dtype size "
                f"{JoltVector.DTYPE_SIZES[dtype]} for dtype {dtype!r}"
            )
        return JoltVector(dtype, data)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        data = encode_bytes(value.data, human_readable=human_readable)
        return {cls.sigil: [value.dtype, data]}


class JoltUnsupportedTypeTransformer(JoltTypeTransformer):
    _supported_types = (JoltUnsupportedType,)
    sigil = "UT"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, list):
            raise JOLTValueError('Expecting array after sigil "UT"')
        if len(value) not in [3, 4]:
            raise JOLTValueError(
                'Expecting array of length 3 or 4 after sigil "UT"'
            )
        if not isinstance(value[0], str):
            raise JOLTValueError(
                "Expecting unsupported type name as string as"
                ' first element of array after sigil "UT"'
            )
        name = value[0]
        if not isinstance(value[1], int):
            raise JOLTValueError(
                "Expecting minimum bolt major version as"
                ' second element of array after sigil "UT"'
            )
        minimum_protocol_major = value[1]
        if not isinstance(value[2], int):
            raise JOLTValueError(
                "Expecting minimum bolt minor version as"
                ' third element of array after sigil "UT"'
            )
        minimum_protocol_minor = value[2]
        if len(value) == 3:
            return JoltUnsupportedType(
                name, minimum_protocol_major, minimum_protocol_minor
            )
        if not isinstance(value[3], str):
            raise JOLTValueError(
                "Expecting message string as fourth element of array "
                'after sigil "UT"'
            )
        message = value[3]
        return JoltUnsupportedType(
            name, minimum_protocol_major, minimum_protocol_minor, message
        )

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        data = encode_bytes(value.data, human_readable=human_readable)
        return {cls.sigil: [value.dtype, data]}


class Codec(_Codec):
    sigil_to_type = {
        cls.sigil: cls
        for cls in globals().values()
        if (inspect.isclass(cls) and issubclass(cls, JoltTypeTransformer)
            and cls.sigil is not None)
    }
    native_to_type = {
        type_: cls
        for cls in globals().values()
        if (inspect.isclass(cls) and issubclass(cls, JoltTypeTransformer)
            and cls._supported_types)
        for type_ in cls._supported_types
    }


decode = Codec.decode
encode_simple = Codec.encode_simple
encode_full = Codec.encode_full


__all__ = [
    Codec,
    decode,
    encode_simple,
    encode_full,
]
