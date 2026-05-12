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
from uuid import UUID

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

from ..v3.codec import JoltVectorTransformer  # noqa: F401
from ..v3.codec import JoltUnsupportedTypeTransformer   # noqa: F401

class JoltUuidTransformer(JoltTypeTransformer):
    _supported_types = (UUID,)
    sigil = "UU"

    @staticmethod
    def _decode_simple(value, decode_cb):
        raise NoSimpleRepresentation()

    @staticmethod
    def _decode_full(value, decode_cb):
        if not isinstance(value, str):
            raise JOLTValueError(f'Expecting UUID string after sigil {JoltUuidTransformer.sigil}')
        return UUID(value)

    @staticmethod
    def _encode_simple(value, encode_cb, human_readable):
        raise NoSimpleRepresentation()

    @classmethod
    def _encode_full(cls, value, encode_cb, human_readable):
        return {cls.sigil: str(value)}

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
