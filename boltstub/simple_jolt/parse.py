import json

from .transformers import (
    decode,
    encode_full,
    encode_simple,
)


def dumps_full(obj, human_readable=False):
    obj = encode_full(obj, human_readable=human_readable)
    return json.dumps(obj)


def dumps_simple(obj, human_readable=False):
    obj = encode_simple(obj, human_readable=human_readable)
    return json.dumps(obj)


def loads(str_):
    obj = json.loads(str_)
    return decode(obj)


__all__ = dumps_full, dumps_simple, loads
