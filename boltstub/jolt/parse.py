from .types import (
    JoltNull,
    JoltBool,
    JoltInt,
    JoltFloat,
    JoltStr,
    JoltBytes,
    JoltList,
    JoltDict,
    JoltDateTime,
    JoltNode,
    JoltRelationship,
    JoltPath
)




def dumps_full(obj, human_readable=False):
    pass


def dumps_simple(obj, human_readable=False):
    pass


def loads(str_):
    pass


__all__ = dumps_full, dumps_simple, loads


if __name__ == "__main__":
    parse('[1, 1.2, {"Z": "1"}]') == [1, 1.2, 1]
