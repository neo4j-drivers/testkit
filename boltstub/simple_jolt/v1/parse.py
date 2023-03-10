import json

from .codec import Codec


def dumps_full(obj, human_readable=False):
    obj = Codec.encode_full(obj, human_readable=human_readable)
    return json.dumps(obj)


def dumps_simple(obj, human_readable=False):
    obj = Codec.encode_simple(obj, human_readable=human_readable)
    return json.dumps(obj)


def loads(str_):
    obj = json.loads(str_)
    return Codec.decode(obj)


__all__ = dumps_full, dumps_simple, loads
